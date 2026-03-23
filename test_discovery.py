#!/usr/bin/env python3
"""Quick test of discovery mode on a specific URL."""

import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from playwright.async_api import async_playwright
from src.database import init_db, AsyncSessionLocal
from src.discovery import CompanyDiscoveryCrawler
from src.utils import create_fresh_sbr_connection
from src.logger import setup_logger

logger = setup_logger("test_discovery", "DEBUG")

TEST_URL = "https://www.linkedin.com/ad-library/search?keyword=salesforce&countries=US&dateOption=last-30-days"

# Use 5 concurrent workers for testing
DISCOVERY_BATCH_SIZE = 5

# Set to True to skip Phase 1 (scroll) and only run Phase 2 (process)
SKIP_SCROLL = "--phase2" in sys.argv


async def main():
    await init_db()

    # Override batch size
    import src.config as config
    config.browser_config.MAX_CONCURRENT_PAGES = DISCOVERY_BATCH_SIZE
    config.MAX_CONCURRENT_PAGES = DISCOVERY_BATCH_SIZE

    async with async_playwright() as playwright:
        crawler = CompanyDiscoveryCrawler(country_code="US", custom_url=TEST_URL)

        if SKIP_SCROLL:
            # Phase 2 only: re-scroll just 2 scrolls to get some unknowns quickly
            logger.info("Phase 2 only mode — quick scroll to get a few unknowns")
            sbr_browser, context, page = await create_fresh_sbr_connection(playwright)
            try:
                async with AsyncSessionLocal() as db:
                    # Quick scroll: just 3 scrolls to get ~100 cards
                    from src.utils import get_known_advertiser_names
                    known_names = await get_known_advertiser_names(db)
                    logger.info(f"Known: {len(known_names)} advertisers")

                    url = TEST_URL
                    await page.goto(url, wait_until='domcontentloaded', timeout=60000)
                    await asyncio.sleep(5)

                    for i in range(5):
                        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                        await asyncio.sleep(3)

                    cards = await page.evaluate("""() => {
                        const cards = document.querySelectorAll('.search-result-item');
                        return Array.from(cards).map(card => {
                            const ariaEl = card.querySelector('[aria-label]');
                            const detailLink = card.querySelector('a[href*="/ad-library/detail/"]');
                            return {
                                ariaLabel: ariaEl ? ariaEl.getAttribute('aria-label') : null,
                                detailUrl: detailLink ? detailLink.href.split('?')[0] : null
                            };
                        });
                    }""")

                    for card in cards:
                        if not card.get('ariaLabel') or not card.get('detailUrl'):
                            continue
                        name = card['ariaLabel'].split(',')[0].strip()
                        if name and name not in known_names and name not in crawler.unknown_advertisers:
                            crawler.unknown_advertisers[name] = card['detailUrl']

                    logger.info(f"Quick scroll found {len(crawler.unknown_advertisers)} unknown advertisers from {len(cards)} cards")
                    # Take only first 15 for a quick test
                    if len(crawler.unknown_advertisers) > 15:
                        items = list(crawler.unknown_advertisers.items())[:15]
                        crawler.unknown_advertisers = dict(items)
                        logger.info(f"Trimmed to {len(crawler.unknown_advertisers)} for quick test")
            finally:
                await sbr_browser.close()
        else:
            # Full Phase 1
            logger.info(f"Full discovery on: {TEST_URL}")
            sbr_browser, context, page = await create_fresh_sbr_connection(playwright)
            try:
                async with AsyncSessionLocal() as db:
                    unknown = await crawler.discover_from_listing(page, db)
                    logger.info(f"Phase 1: {len(unknown)} unknown from {crawler.total_cards_seen} cards")
            finally:
                await sbr_browser.close()

        # Phase 2: Process unknowns
        if crawler.unknown_advertisers:
            logger.info(f"\n--- Phase 2: Processing {len(crawler.unknown_advertisers)} unknown advertisers ---")
            for name, url in list(crawler.unknown_advertisers.items())[:5]:
                logger.info(f"  Will process: {name} -> {url}")

            async with AsyncSessionLocal() as db:
                processed = await crawler.process_unknown_advertisers(db, playwright)
                logger.info(f"Processed: {processed} companies")

        # Final DB state
        async with AsyncSessionLocal() as db:
            from sqlalchemy import text
            result = await db.execute(text("SELECT COUNT(*) FROM companies"))
            total = result.scalar()
            result2 = await db.execute(text("SELECT COUNT(*) FROM companies WHERE first_seen_country = 'US'"))
            us = result2.scalar()
            logger.info(f"\nTotal companies in DB: {total} | From US: {us}")

            result = await db.execute(text(
                "SELECT advertiser_name, company_id, ad_type, company_url, promoted_by_name "
                "FROM companies WHERE first_seen_country = 'US' ORDER BY id DESC LIMIT 20"
            ))
            rows = result.fetchall()
            logger.info(f"\nDiscovered companies (US):")
            for row in rows:
                promoted = f" [promoted by: {row[4]}]" if row[4] else ""
                url = row[3] or "no url"
                logger.info(f"  {row[0]} | company_id={row[1]} | {row[2]} | {url}{promoted}")


if __name__ == "__main__":
    asyncio.run(main())
