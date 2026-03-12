from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from playwright.async_api import async_playwright
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from datetime import datetime
import logging
import json
from contextlib import asynccontextmanager

from src.utils import init_db, generate_linkedin_url, setup_browser_context, backfill_companies_from_ads
from src.models import LinkedInAd, Company
from src.crawler import AsyncLinkedInCrawler
from src.discovery import run_parallel_discovery, run_impression_range_discovery
from src.logger import setup_logger
from src.database import Base, engine, AsyncSessionLocal, get_db
from src.config import brightdata_config, crawler_config

logger = setup_logger("linkedin_crawler", log_level=logging.INFO)

# Track background scraping jobs
_active_jobs: dict = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(lifespan=lifespan, title="LinkedIn Ad Scraper")


@app.get("/")
async def root():
    return {
        "service": "LinkedIn Ad Scraper",
        "brightdata": f"{brightdata_config.get_mode().upper()} ACTIVE" if brightdata_config.is_configured() else "NOT CONFIGURED",
        "endpoints": {
            "/crawl?company_id=X": "Start scraping (runs in background)",
            "/status/{job_id}": "Check scraping job status",
            "/discover?countries=US,UK": "Discover by country",
            "/discover?url=URL&impressions_start=10000": "Discover by impression ranges",
            "/companies": "List all discovered companies",
            "/check-ads/{company_id}": "Get all ads for a company",
            "/check-ad/{ad_id}": "Get a specific ad",
            "/export/{company_id}": "Export ads as JSON",
            "/health": "Health check",
        }
    }


@app.get("/health")
async def health_check(db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(text("SELECT 1"))
        row = result.scalar()
        return {"status": "healthy", "database": "connected", "test_query": row}
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=503, detail=str(e))


async def _run_scrape(company_id: str, job_id: str):
    """Background scraping task."""
    _active_jobs[job_id]["status"] = "running"
    try:
        crawler = AsyncLinkedInCrawler(company_id)
        async with async_playwright() as playwright:
            browser, context = await setup_browser_context(playwright)
            page = await context.new_page()

            await crawler.collect_ad_urls(page)
            _active_jobs[job_id]["urls_found"] = len(crawler.detail_urls)

            async with AsyncSessionLocal() as db:
                processed_count = await crawler.process_all_ads(page, db, playwright=playwright)

            await browser.close()

        _active_jobs[job_id]["status"] = "completed"
        _active_jobs[job_id]["processed_ads"] = processed_count
        _active_jobs[job_id]["completed_at"] = datetime.now().isoformat()
        logger.info(f"Job {job_id}: Completed — {processed_count} ads processed")

    except Exception as e:
        _active_jobs[job_id]["status"] = "failed"
        _active_jobs[job_id]["error"] = str(e)
        logger.error(f"Job {job_id}: Failed — {str(e)}")


@app.get("/crawl")
async def crawl(company_id: str, background_tasks: BackgroundTasks):
    """Start a scraping job in the background. Returns a job_id to track progress."""
    job_id = f"{company_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    _active_jobs[job_id] = {
        "company_id": company_id,
        "status": "queued",
        "started_at": datetime.now().isoformat(),
        "urls_found": 0,
        "processed_ads": 0,
    }

    background_tasks.add_task(_run_scrape, company_id, job_id)

    return {
        "status": "started",
        "job_id": job_id,
        "track_at": f"/status/{job_id}",
        "message": f"Scraping started for company {company_id}. Close your browser — it runs in the cloud."
    }


@app.get("/status/{job_id}")
async def job_status(job_id: str):
    """Check the status of a scraping job."""
    if job_id not in _active_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return _active_jobs[job_id]


@app.get("/jobs")
async def list_jobs():
    """List all scraping jobs."""
    return _active_jobs


@app.get("/check-ads/{company_id}")
async def check_ads(company_id: str, db: AsyncSession = Depends(get_db)):
    try:
        result = await db.execute(
            text("SELECT * FROM linkedin_ads WHERE company_id = :company_id"),
            {"company_id": int(company_id)}
        )
        ads = result.mappings().all()
        return {
            "total_ads": len(ads),
            "ads": [{
                "ad_id": ad['ad_id'],
                "advertiser_name": ad['advertiser_name'],
                "headline": ad['headline'],
                "campaign_start_date": str(ad['campaign_start_date']),
                "campaign_end_date": str(ad['campaign_end_date']),
                "campaign_impressions_range": ad['campaign_impressions_range'],
            } for ad in ads]
        }
    except Exception as e:
        logger.error(f"Error checking ads: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/check-ad/{ad_id}")
async def check_ad(ad_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(
        text("SELECT * FROM linkedin_ads WHERE ad_id = :ad_id"),
        {"ad_id": ad_id}
    )
    ad = result.mappings().first()
    if not ad:
        raise HTTPException(status_code=404, detail="Ad not found")
    return dict(ad)


async def _run_discovery_countries(country_codes: list, job_id: str, batch_size: int = 5):
    """Background discovery task for country-based discovery."""
    _active_jobs[job_id]["status"] = "running"
    try:
        import src.config as config
        config.browser_config.MAX_CONCURRENT_PAGES = batch_size
        config.MAX_CONCURRENT_PAGES = batch_size

        async with async_playwright() as playwright:
            results = await run_parallel_discovery(
                country_codes,
                max_parallel=crawler_config.DISCOVERY_CONCURRENT_COUNTRIES,
                playwright=playwright
            )

        _active_jobs[job_id]["status"] = "completed"
        _active_jobs[job_id]["results"] = results
        _active_jobs[job_id]["completed_at"] = datetime.now().isoformat()
        logger.info(f"Job {job_id}: Discovery completed")

    except Exception as e:
        _active_jobs[job_id]["status"] = "failed"
        _active_jobs[job_id]["error"] = str(e)
        logger.error(f"Job {job_id}: Discovery failed — {str(e)}")


async def _run_discovery_url(custom_url: str, job_id: str, batch_size: int = 5):
    """Background discovery task for custom URL discovery."""
    from src.discovery import CompanyDiscoveryCrawler
    from src.utils import create_fresh_sbr_connection
    import src.config as config

    _active_jobs[job_id]["status"] = "running"
    try:
        config.browser_config.MAX_CONCURRENT_PAGES = batch_size
        config.MAX_CONCURRENT_PAGES = batch_size

        async with async_playwright() as playwright:
            crawler = CompanyDiscoveryCrawler(country_code="CUSTOM", custom_url=custom_url)

            # Phase 1: Scroll
            sbr_browser, context, page = await create_fresh_sbr_connection(playwright)
            try:
                async with AsyncSessionLocal() as db:
                    unknown = await crawler.discover_from_listing(page, db)
            finally:
                await sbr_browser.close()

            _active_jobs[job_id]["phase1"] = {
                "cards_seen": crawler.total_cards_seen,
                "unknown_found": len(unknown),
                "known_skipped": crawler.known_count,
            }
            logger.info(
                f"Job {job_id}: Scroll done — {crawler.total_cards_seen} cards, "
                f"{len(unknown)} unknown, {crawler.known_count} known skipped"
            )

            # Phase 2: Process unknowns
            if unknown:
                _active_jobs[job_id]["status"] = "processing_details"
                async with AsyncSessionLocal() as db:
                    processed = await crawler.process_unknown_advertisers(db, playwright)
                _active_jobs[job_id]["phase2"] = {"processed": processed}
            else:
                _active_jobs[job_id]["phase2"] = {"processed": 0, "note": "all advertisers already known"}

        _active_jobs[job_id]["status"] = "completed"
        _active_jobs[job_id]["completed_at"] = datetime.now().isoformat()
        logger.info(f"Job {job_id}: Discovery completed")

    except Exception as e:
        _active_jobs[job_id]["status"] = "failed"
        _active_jobs[job_id]["error"] = str(e)
        logger.error(f"Job {job_id}: Discovery failed — {str(e)}")


async def _run_discovery_impressions(
    base_url: str, job_id: str, start: int, step: int, count: int, batch_size: int
):
    """Background task for impression-range partitioned discovery."""
    _active_jobs[job_id]["status"] = "running"
    try:
        async def progress_callback(label, phase, stats):
            _active_jobs[job_id]["current_range"] = label
            _active_jobs[job_id]["current_phase"] = phase
            _active_jobs[job_id]["range_stats"] = _active_jobs[job_id].get("range_stats", {})
            _active_jobs[job_id]["range_stats"][label] = stats

        async with async_playwright() as playwright:
            results = await run_impression_range_discovery(
                base_url=base_url,
                start=start,
                step=step,
                count=count,
                batch_size=batch_size,
                playwright=playwright,
                progress_callback=progress_callback,
            )

        _active_jobs[job_id]["status"] = "completed"
        _active_jobs[job_id]["results"] = results.get("totals", {})
        _active_jobs[job_id]["completed_at"] = datetime.now().isoformat()
        logger.info(f"Job {job_id}: Impression range discovery completed")

    except Exception as e:
        _active_jobs[job_id]["status"] = "failed"
        _active_jobs[job_id]["error"] = str(e)
        logger.error(f"Job {job_id}: Impression range discovery failed — {str(e)}")


@app.get("/discover")
async def discover(
    background_tasks: BackgroundTasks,
    countries: str = None,
    url: str = None,
    batch_size: int = 5,
    impressions_start: int = None,
    impressions_step: int = 1000,
    impressions_count: int = 50,
):
    """Start a company discovery job.

    Modes:
    - ?countries=US,UK — discover by country
    - ?url=<linkedin_ad_library_url> — discover from custom URL
    - ?url=<base_url>&impressions_start=10000 — partition by impression ranges

    Options:
    - &batch_size=5 — concurrent detail page sessions
    - &impressions_step=1000 — impression range step size (default 1000)
    - &impressions_count=50 — number of ranges to process (default 50)
    """
    if not countries and not url:
        raise HTTPException(status_code=400, detail="Provide either 'countries' or 'url' parameter")

    job_id = f"discover_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    if url and impressions_start is not None:
        # Impression-range partitioned discovery
        _active_jobs[job_id] = {
            "type": "discovery_impressions",
            "base_url": url,
            "impressions_start": impressions_start,
            "impressions_step": impressions_step,
            "impressions_count": impressions_count,
            "batch_size": batch_size,
            "status": "queued",
            "started_at": datetime.now().isoformat(),
        }
        background_tasks.add_task(
            _run_discovery_impressions, url, job_id,
            impressions_start, impressions_step, impressions_count, batch_size
        )
    elif url:
        _active_jobs[job_id] = {
            "type": "discovery_url",
            "url": url,
            "batch_size": batch_size,
            "status": "queued",
            "started_at": datetime.now().isoformat(),
        }
        background_tasks.add_task(_run_discovery_url, url, job_id, batch_size)
    else:
        country_list = [c.strip().upper() for c in countries.split(",")]
        _active_jobs[job_id] = {
            "type": "discovery_countries",
            "countries": country_list,
            "batch_size": batch_size,
            "status": "queued",
            "started_at": datetime.now().isoformat(),
        }
        background_tasks.add_task(_run_discovery_countries, country_list, job_id, batch_size)

    return {"status": "started", "job_id": job_id, "track_at": f"/status/{job_id}"}


@app.get("/companies")
async def list_companies(
    limit: int = 100,
    offset: int = 0,
    ad_type: str = None,
    db: AsyncSession = Depends(get_db)
):
    """List discovered companies."""
    query = "SELECT * FROM companies"
    params = {}
    if ad_type:
        query += " WHERE ad_type = :ad_type"
        params["ad_type"] = ad_type
    query += " ORDER BY created_at DESC LIMIT :limit OFFSET :offset"
    params["limit"] = limit
    params["offset"] = offset

    result = await db.execute(text(query), params)
    companies = result.mappings().all()

    count_result = await db.execute(text("SELECT COUNT(*) FROM companies"))
    total = count_result.scalar()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "companies": [dict(c) for c in companies]
    }


@app.get("/companies/{advertiser_name}")
async def get_company(advertiser_name: str, db: AsyncSession = Depends(get_db)):
    """Get a specific company by advertiser name."""
    result = await db.execute(
        text("SELECT * FROM companies WHERE advertiser_name = :name"),
        {"name": advertiser_name}
    )
    company = result.mappings().first()
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return dict(company)


@app.get("/export/companies")
async def export_companies_api(
    ad_type: str = None,
    db: AsyncSession = Depends(get_db)
):
    """Export all discovered companies as JSON."""
    query = "SELECT * FROM companies"
    params = {}
    if ad_type:
        query += " WHERE ad_type = :ad_type"
        params["ad_type"] = ad_type
    query += " ORDER BY id"
    result = await db.execute(text(query), params)
    companies = result.mappings().all()
    data = []
    for c in companies:
        d = dict(c)
        for k, v in d.items():
            if hasattr(v, 'isoformat'):
                d[k] = v.isoformat()
        data.append(d)
    return {"total": len(data), "companies": data}


@app.get("/export/{company_id}")
async def export_ads(company_id: str, db: AsyncSession = Depends(get_db)):
    """Export all ads for a company as JSON."""
    try:
        result = await db.execute(
            text("SELECT * FROM linkedin_ads WHERE company_id = :company_id"),
            {"company_id": int(company_id)}
        )
        ads = result.mappings().all()
        data = []
        for ad in ads:
            d = dict(ad)
            for k, v in d.items():
                if hasattr(v, 'isoformat'):
                    d[k] = v.isoformat()
            data.append(d)
        return {"total_ads": len(data), "ads": data}
    except Exception as e:
        logger.error(f"Export error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
