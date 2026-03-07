#!/usr/bin/env python3
"""
CLI tool to scrape LinkedIn Ad Library.

Usage:
    # Discover companies running ads (by country):
    python scrape.py discover --countries US,UK,DE,FR
    python scrape.py discover --countries US --parallel 5

    # Discover with a custom URL (keyword, date filters, etc.):
    python scrape.py discover --url "https://www.linkedin.com/ad-library/search?keyword=salesforce&countries=US&dateOption=last-30-days"
    python scrape.py discover --url "https://www.linkedin.com/ad-library/search?keyword=crm" --batch-size 10

    # Scrape by company ID:
    python scrape.py scrape 1234567

    # Scrape by company name:
    python scrape.py scrape microsoft

    # Scrape from a full LinkedIn Ad Library URL:
    python scrape.py scrape "https://www.linkedin.com/ad-library/search?companyIds=1234567"

    # Export results as JSON:
    python scrape.py scrape 1234567 --export json

    # Backfill companies table from existing ads:
    python scrape.py backfill

    # Backward compatible (no subcommand = scrape):
    python scrape.py 1234567
"""

import asyncio
import sys
import os
import re
import json
import csv
import argparse
import logging

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from playwright.async_api import async_playwright
from src.database import init_db, AsyncSessionLocal
from src.crawler import AsyncLinkedInCrawler
from src.utils import setup_browser_context, backfill_companies_from_ads
from src.config import brightdata_config, crawler_config
from src.logger import setup_logger
from sqlalchemy import text


logger = setup_logger("scrape_cli", "INFO")


def parse_input(raw_input: str) -> str:
    """Extract company_id from a URL or return raw input as company_id/name."""
    match = re.search(r'companyIds=(\d+)', raw_input)
    if match:
        return match.group(1)

    match = re.search(r'accountOwner=([^&]+)', raw_input)
    if match:
        return match.group(1)

    match = re.search(r'linkedin\.com/company/(\d+)', raw_input)
    if match:
        return match.group(1)

    return raw_input.strip()


async def export_results(company_id: str, fmt: str):
    """Export scraped data to JSON or CSV."""
    async with AsyncSessionLocal() as db:
        try:
            cid = int(company_id)
            result = await db.execute(
                text("SELECT * FROM linkedin_ads WHERE company_id = :cid"),
                {"cid": cid}
            )
        except ValueError:
            result = await db.execute(text("SELECT * FROM linkedin_ads"))

        rows = result.mappings().all()

        if not rows:
            logger.warning("No ads found to export")
            return

        filename = f"ads_{company_id}"

        if fmt == "json":
            filepath = f"{filename}.json"
            data = []
            for row in rows:
                d = dict(row)
                for k, v in d.items():
                    if hasattr(v, 'isoformat'):
                        d[k] = v.isoformat()
                data.append(d)

            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Exported {len(data)} ads to {filepath}")

        elif fmt == "csv":
            filepath = f"{filename}.csv"
            if rows:
                keys = list(rows[0].keys())
                with open(filepath, 'w', newline='') as f:
                    writer = csv.DictWriter(f, fieldnames=keys)
                    writer.writeheader()
                    for row in rows:
                        writer.writerow({k: str(v) if v is not None else '' for k, v in dict(row).items()})
                logger.info(f"Exported {len(rows)} ads to {filepath}")


async def show_summary(company_id: str):
    """Print a summary of scraped ads."""
    async with AsyncSessionLocal() as db:
        try:
            cid = int(company_id)
            result = await db.execute(
                text("SELECT COUNT(*) as cnt FROM linkedin_ads WHERE company_id = :cid"),
                {"cid": cid}
            )
        except ValueError:
            result = await db.execute(text("SELECT COUNT(*) as cnt FROM linkedin_ads"))

        count = result.scalar()
        logger.info(f"Total ads in database for {company_id}: {count}")


async def run_scrape(args):
    """Run the per-company scrape flow."""
    company_id = parse_input(args.company)
    logger.info(f"Company ID/Name: {company_id}")

    mode = brightdata_config.get_mode()
    if mode == "scraping_browser":
        logger.info("BrightData Scraping Browser: ACTIVE")
    elif mode == "residential_proxy":
        logger.info(f"BrightData Residential Proxy: ACTIVE ({brightdata_config.HOST}:{brightdata_config.PORT})")
    else:
        logger.warning("No BrightData configured — scraping without proxy rotation")

    await init_db()

    if args.export_only:
        await export_results(company_id, args.export_only)
        return

    crawler = AsyncLinkedInCrawler(company_id)

    async with async_playwright() as playwright:
        browser, context = await setup_browser_context(playwright)
        page = await context.new_page()

        try:
            logger.info("Step 1/2: Collecting ad URLs...")
            await crawler.collect_ad_urls(page)

            if not crawler.detail_urls:
                logger.warning("No ad URLs found.")
                await browser.close()
                return

            logger.info(f"Found {len(crawler.detail_urls)} ad URLs")

            logger.info("Step 2/2: Processing ad details...")
            async with AsyncSessionLocal() as db:
                processed = await crawler.process_all_ads(page, db, playwright=playwright)
                logger.info(f"Successfully processed {processed} ads")

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            raise
        finally:
            await browser.close()

    await show_summary(company_id)

    if args.export:
        await export_results(company_id, args.export)


async def run_discover(args):
    """Run the company discovery flow."""
    from src.discovery import run_parallel_discovery, CompanyDiscoveryCrawler
    from src.utils import create_fresh_sbr_connection
    import src.config as config

    if not args.countries and not args.url:
        logger.error("You must provide either --countries or --url")
        return

    mode = brightdata_config.get_mode()
    if mode != "scraping_browser":
        logger.error("Discovery mode requires BrightData Scraping Browser (SBR_WS_ENDPOINT)")
        return

    logger.info("BrightData Scraping Browser: ACTIVE")
    await init_db()

    # Override batch size for detail processing
    batch_size = args.batch_size
    config.browser_config.MAX_CONCURRENT_PAGES = batch_size
    config.MAX_CONCURRENT_PAGES = batch_size

    if args.url:
        # Custom URL mode: single session with the provided URL
        logger.info(f"Discovery mode: custom URL = {args.url}")
        logger.info(f"Detail processing batch size: {batch_size}")

        async with async_playwright() as playwright:
            crawler = CompanyDiscoveryCrawler(country_code="CUSTOM", custom_url=args.url)

            # Phase 1: Scroll
            sbr_browser, context, page = await create_fresh_sbr_connection(playwright)
            try:
                async with AsyncSessionLocal() as db:
                    unknown = await crawler.discover_from_listing(page, db)
            finally:
                await sbr_browser.close()

            logger.info(
                f"Scroll complete: {crawler.total_cards_seen} cards, "
                f"{len(unknown)} unknown, {crawler.known_count} known skipped"
            )

            # Phase 2: Process unknowns
            if unknown:
                logger.info(f"Processing {len(unknown)} unknown advertisers...")
                async with AsyncSessionLocal() as db:
                    processed = await crawler.process_unknown_advertisers(db, playwright)
                    logger.info(f"Processed: {processed} companies")
            else:
                logger.info("All advertisers already known — nothing to process")

    else:
        # Country-parallel mode
        countries = [c.strip().upper() for c in args.countries.split(",")]
        parallel = args.parallel
        logger.info(f"Discovery mode: countries={countries}, parallel={parallel}")
        logger.info(f"Detail processing batch size: {batch_size}")

        async with async_playwright() as playwright:
            results = await run_parallel_discovery(countries, parallel, playwright)

        logger.info("\n--- Discovery Results ---")
        for country, stats in results.items():
            if 'error' in stats:
                logger.error(f"  {country}: FAILED - {stats['error']}")
            else:
                logger.info(
                    f"  {country}: {stats['cards_seen']} cards seen, "
                    f"{stats['unknown_found']} unknown, "
                    f"{stats['known_skipped']} known, "
                    f"{stats['processed']} processed"
                )

    # Show total companies in DB
    async with AsyncSessionLocal() as db:
        result = await db.execute(text("SELECT COUNT(*) FROM companies"))
        total = result.scalar()
        logger.info(f"\nTotal companies in database: {total}")

    # Export if requested
    if args.export:
        await export_companies(args.export)


async def export_companies(fmt: str, ad_type_filter: str = None):
    """Export companies table to JSON or CSV."""
    async with AsyncSessionLocal() as db:
        query = "SELECT * FROM companies"
        params = {}
        if ad_type_filter:
            query += " WHERE ad_type = :ad_type"
            params["ad_type"] = ad_type_filter
        query += " ORDER BY id"

        result = await db.execute(text(query), params)
        rows = result.mappings().all()

        if not rows:
            logger.warning("No companies found to export")
            return

        suffix = f"_{ad_type_filter}" if ad_type_filter else ""
        filename = f"companies{suffix}"

        if fmt == "json":
            filepath = f"{filename}.json"
            data = []
            for row in rows:
                d = dict(row)
                for k, v in d.items():
                    if hasattr(v, 'isoformat'):
                        d[k] = v.isoformat()
                data.append(d)
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Exported {len(data)} companies to {filepath}")

        elif fmt == "csv":
            filepath = f"{filename}.csv"
            keys = list(rows[0].keys())
            with open(filepath, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=keys)
                writer.writeheader()
                for row in rows:
                    writer.writerow({k: str(v) if v is not None else '' for k, v in dict(row).items()})
            logger.info(f"Exported {len(rows)} companies to {filepath}")


async def run_backfill(args):
    """Backfill companies table from existing ads."""
    await init_db()
    async with AsyncSessionLocal() as db:
        count = await backfill_companies_from_ads(db)
        logger.info(f"Backfill complete: {count} new companies added")


async def main():
    parser = argparse.ArgumentParser(
        description="LinkedIn Ad Library Scraper & Company Discovery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    subparsers = parser.add_subparsers(dest="command")

    # discover subcommand
    discover_parser = subparsers.add_parser(
        "discover", help="Discover companies running ads by scrolling listing pages"
    )
    discover_parser.add_argument(
        "--countries",
        help="Comma-separated country codes (e.g., US,UK,DE,FR)"
    )
    discover_parser.add_argument(
        "--url",
        help="Custom LinkedIn Ad Library URL with filters (keyword, date, etc.)"
    )
    discover_parser.add_argument(
        "--parallel", type=int,
        default=crawler_config.DISCOVERY_CONCURRENT_COUNTRIES,
        help=f"Max parallel country sessions (default: {crawler_config.DISCOVERY_CONCURRENT_COUNTRIES})"
    )
    discover_parser.add_argument(
        "--batch-size", type=int, default=5,
        help="Concurrent SBR sessions for detail page processing (default: 5)"
    )
    discover_parser.add_argument(
        "--export", choices=["json", "csv"],
        help="Export companies after discovery"
    )
    discover_parser.add_argument("-v", "--verbose", action="store_true")

    # export-companies subcommand
    export_co_parser = subparsers.add_parser(
        "export-companies", help="Export companies table to JSON or CSV"
    )
    export_co_parser.add_argument(
        "format", choices=["json", "csv"], help="Export format"
    )
    export_co_parser.add_argument(
        "--type", choices=["company_ad", "personal_ad"],
        help="Filter by ad type"
    )
    export_co_parser.add_argument("-v", "--verbose", action="store_true")

    # scrape subcommand
    scrape_parser = subparsers.add_parser(
        "scrape", help="Scrape all ads for a specific company"
    )
    scrape_parser.add_argument("company", help="Company ID, company name, or full URL")
    scrape_parser.add_argument("--export", choices=["json", "csv"])
    scrape_parser.add_argument("--export-only", choices=["json", "csv"])
    scrape_parser.add_argument("-v", "--verbose", action="store_true")

    # backfill subcommand
    backfill_parser = subparsers.add_parser(
        "backfill", help="Backfill companies table from existing ads data"
    )
    backfill_parser.add_argument("-v", "--verbose", action="store_true")

    args = parser.parse_args()

    if hasattr(args, 'verbose') and args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.command == "discover":
        await run_discover(args)
    elif args.command == "scrape":
        await run_scrape(args)
    elif args.command == "backfill":
        await run_backfill(args)
    elif args.command == "export-companies":
        await init_db()
        await export_companies(args.format, args.type)
    elif args.command is None:
        # Backward compatibility: python scrape.py <company_id>
        if len(sys.argv) > 1 and not sys.argv[1].startswith('-'):
            # Re-parse as scrape subcommand
            sys.argv.insert(1, "scrape")
            args = parser.parse_args()
            if hasattr(args, 'verbose') and args.verbose:
                logging.getLogger().setLevel(logging.DEBUG)
            await run_scrape(args)
        else:
            parser.print_help()


if __name__ == "__main__":
    asyncio.run(main())
