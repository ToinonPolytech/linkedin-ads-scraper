#!/usr/bin/env python3
"""
CLI tool to scrape LinkedIn Ad Library.

Usage:
    # Scrape by company ID:
    python scrape.py 1234567

    # Scrape by company name:
    python scrape.py microsoft

    # Scrape from a full LinkedIn Ad Library URL:
    python scrape.py "https://www.linkedin.com/ad-library/search?companyIds=1234567"

    # Export results as JSON:
    python scrape.py 1234567 --export json

    # Export results as CSV:
    python scrape.py 1234567 --export csv
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
from src.utils import setup_browser_context
from src.config import brightdata_config
from src.logger import setup_logger
from sqlalchemy import text


logger = setup_logger("scrape_cli", "INFO")


def parse_input(raw_input: str) -> str:
    """Extract company_id from a URL or return raw input as company_id/name."""
    # Full URL: extract companyIds param
    match = re.search(r'companyIds=(\d+)', raw_input)
    if match:
        return match.group(1)

    # Full URL: extract accountOwner param
    match = re.search(r'accountOwner=([^&]+)', raw_input)
    if match:
        return match.group(1)

    # URL with /company/ID pattern
    match = re.search(r'linkedin\.com/company/(\d+)', raw_input)
    if match:
        return match.group(1)

    # Plain ID or company name
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


async def main():
    parser = argparse.ArgumentParser(
        description="Scrape LinkedIn Ad Library",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument("company", help="Company ID, company name, or full LinkedIn Ad Library URL")
    parser.add_argument("--export", choices=["json", "csv"], help="Export results after scraping")
    parser.add_argument("--export-only", choices=["json", "csv"], help="Export existing data without scraping")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    company_id = parse_input(args.company)
    logger.info(f"Company ID/Name: {company_id}")

    # Show proxy status
    mode = brightdata_config.get_mode()
    if mode == "scraping_browser":
        logger.info("BrightData Scraping Browser: ACTIVE (cloud browser + auto proxy rotation)")
    elif mode == "residential_proxy":
        logger.info(f"BrightData Residential Proxy: ACTIVE ({brightdata_config.HOST}:{brightdata_config.PORT})")
    else:
        logger.warning("No BrightData configured — scraping without proxy rotation")
        logger.warning("Set SBR_WS_ENDPOINT in .env for Scraping Browser")

    # Init database
    await init_db()

    # Export-only mode
    if args.export_only:
        await export_results(company_id, args.export_only)
        return

    # Scrape
    crawler = AsyncLinkedInCrawler(company_id)

    async with async_playwright() as playwright:
        browser, context = await setup_browser_context(playwright)
        page = await context.new_page()

        try:
            # Step 1: Collect ad URLs
            logger.info("Step 1/2: Collecting ad URLs...")
            await crawler.collect_ad_urls(page)

            if not crawler.detail_urls:
                logger.warning("No ad URLs found. The company might have no ads or the page structure changed.")
                await browser.close()
                return

            logger.info(f"Found {len(crawler.detail_urls)} ad URLs")

            # Step 2: Process each ad
            logger.info("Step 2/2: Processing ad details...")
            async with AsyncSessionLocal() as db:
                processed = await crawler.process_all_ads(page, db)
                logger.info(f"Successfully processed {processed} ads")

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.error(f"Scraping failed: {str(e)}")
            raise
        finally:
            await browser.close()

    # Show summary
    await show_summary(company_id)

    # Export if requested
    if args.export:
        await export_results(company_id, args.export)


if __name__ == "__main__":
    asyncio.run(main())
