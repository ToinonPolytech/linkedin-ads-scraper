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
from src.discovery import run_parallel_discovery
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
            "/discover?countries=US,UK": "Start company discovery job",
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


async def _run_discovery(country_codes: list, job_id: str):
    """Background discovery task."""
    _active_jobs[job_id]["status"] = "running"
    try:
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


@app.get("/discover")
async def discover(countries: str, background_tasks: BackgroundTasks):
    """Start a company discovery job. Scrolls listing pages by country."""
    country_list = [c.strip().upper() for c in countries.split(",")]
    job_id = f"discover_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    _active_jobs[job_id] = {
        "type": "discovery",
        "countries": country_list,
        "status": "queued",
        "started_at": datetime.now().isoformat(),
    }
    background_tasks.add_task(_run_discovery, country_list, job_id)
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
