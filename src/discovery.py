"""
Company Discovery Crawler for LinkedIn Ad Library.

Instead of scraping every ad detail page, this module:
1. Scrolls listing pages by country
2. Extracts advertiser names from card aria-labels
3. Checks against known companies in the database
4. Only visits detail pages for unknown advertisers (one per advertiser)
5. Extracts company info + "promoted by" relationships
"""

import asyncio
import re
import time
import logging
from playwright.async_api import Page

from src.config import (
    crawler_config,
    browser_config,
    brightdata_config,
    MAX_CONCURRENT_PAGES,
    NAVIGATION_TIMEOUT,
)
from src.utils import (
    generate_discovery_url,
    get_known_advertiser_names,
    upsert_company,
    create_fresh_sbr_connection,
    clean_text,
)
from src.database import AsyncSessionLocal
from src.logger import setup_logger

logger = setup_logger("discovery", "INFO")


class CompanyDiscoveryCrawler:
    """Discovers unknown advertisers from LinkedIn Ad Library listing pages."""

    def __init__(self, country_code: str = "US", custom_url: str = None):
        self.country_code = country_code.upper()
        self.custom_url = custom_url
        self.unknown_advertisers = {}  # {advertiser_name: detail_url}
        self.known_count = 0
        self.total_cards_seen = 0
        self.logger = setup_logger(f"discovery_{self.country_code}")

    async def discover_from_listing(self, page: Page, db) -> dict:
        """Scroll listing page, identify unknown advertisers.

        Returns dict of {advertiser_name: detail_url} for unknowns.
        """
        known_names = await get_known_advertiser_names(db)
        self.logger.info(f"Loaded {len(known_names)} known advertisers from DB")

        url = self.custom_url if self.custom_url else generate_discovery_url(self.country_code)
        self.logger.info(f"Navigating to {url}")

        await page.goto(
            url,
            wait_until='domcontentloaded',
            timeout=browser_config.NAVIGATION_TIMEOUT
        )
        await asyncio.sleep(crawler_config.INITIAL_WAIT_TIME)

        previous_card_count = 0
        consecutive_unchanged = 0
        scroll_count = 0
        seen_names = set()  # track names seen this session to avoid re-processing

        while True:
            self.logger.debug(f"Scroll iteration {scroll_count}")

            try:
                await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                wait_time = crawler_config.BASE_SCROLL_WAIT + (consecutive_unchanged * crawler_config.SCROLL_INCREMENT)
                await asyncio.sleep(wait_time)

                # Extract card data: aria-label + detail link
                cards_data = await page.evaluate("""() => {
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

                current_card_count = len(cards_data)
                new_cards = current_card_count - previous_card_count

                # Process new cards
                for card in cards_data:
                    if not card.get('ariaLabel') or not card.get('detailUrl'):
                        continue

                    # Parse advertiser name: first segment before comma
                    name = card['ariaLabel'].split(',')[0].strip()
                    if not name:
                        continue

                    self.total_cards_seen += 1

                    if name in seen_names:
                        continue
                    seen_names.add(name)

                    if name in known_names:
                        self.known_count += 1
                        continue

                    if name not in self.unknown_advertisers:
                        self.unknown_advertisers[name] = card['detailUrl']

                if new_cards > 0:
                    self.logger.info(
                        f"[{self.country_code}] Scroll {scroll_count}: "
                        f"{current_card_count} cards total, "
                        f"{len(self.unknown_advertisers)} unknown advertisers, "
                        f"{self.known_count} known skipped"
                    )
                    consecutive_unchanged = 0
                else:
                    consecutive_unchanged += 1

                previous_card_count = current_card_count

            except Exception as e:
                self.logger.error(f"Error during scroll: {str(e)}")
                consecutive_unchanged += 1

            scroll_count += 1

            # Termination: no new cards after several scrolls
            if consecutive_unchanged >= crawler_config.MAX_UNCHANGED_SCROLLS:
                # Final check: scroll to top and back
                try:
                    await page.evaluate('window.scrollTo(0, 0)')
                    await asyncio.sleep(crawler_config.SCROLL_WAIT_TIME)
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                    await asyncio.sleep(crawler_config.SCROLL_WAIT_TIME)

                    final_data = await page.evaluate("""() => {
                        const cards = document.querySelectorAll('.search-result-item');
                        return cards.length;
                    }""")

                    if final_data > current_card_count:
                        self.logger.info(f"Final check found more cards, continuing...")
                        consecutive_unchanged = 0
                        continue

                    self.logger.info("No new cards in final check, done scrolling")
                    break
                except Exception:
                    break

            if scroll_count > crawler_config.DISCOVERY_MAX_SCROLL_COUNT:
                self.logger.warning(f"Reached max scroll count ({crawler_config.DISCOVERY_MAX_SCROLL_COUNT})")
                break

        self.logger.info(
            f"[{self.country_code}] Scroll complete: "
            f"{self.total_cards_seen} cards seen, "
            f"{len(self.unknown_advertisers)} unknown advertisers to process, "
            f"{self.known_count} known skipped"
        )
        return self.unknown_advertisers

    async def process_unknown_advertisers(self, db, playwright) -> int:
        """Visit detail pages for unknown advertisers, extract company info."""
        total = len(self.unknown_advertisers)
        if total == 0:
            self.logger.info("No unknown advertisers to process")
            return 0

        batch_size = min(MAX_CONCURRENT_PAGES, total)
        self.logger.info(
            f"Processing {total} unknown advertisers "
            f"(batch size: {batch_size})"
        )

        processed = 0
        new_companies = 0
        updated_companies = 0
        failed = 0
        db_lock = asyncio.Lock()
        items = list(self.unknown_advertisers.items())
        batch_delay = 3

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(items) + batch_size - 1) // batch_size

            async def process_one(name_url_pair):
                nonlocal processed, new_companies, updated_companies, failed
                name, url = name_url_pair
                sbr_browser = None
                try:
                    sbr_browser, context, page = await create_fresh_sbr_connection(playwright)
                    company_info = await self._extract_company_info(page, url, name)

                    if company_info:
                        async with db_lock:
                            status = await upsert_company(db, company_info)
                            if status == 'new':
                                new_companies += 1
                            elif status == 'updated':
                                updated_companies += 1
                            processed += 1
                    else:
                        failed += 1

                except Exception as e:
                    self.logger.error(f"Failed for {name}: {str(e)}")
                    failed += 1
                finally:
                    if sbr_browser:
                        try:
                            await sbr_browser.close()
                        except Exception:
                            pass

            tasks = [process_one(item) for item in batch]
            await asyncio.gather(*tasks, return_exceptions=True)

            self.logger.info(
                f"Batch {batch_num}/{total_batches} done — "
                f"Processed: {processed}/{total}, "
                f"New: {new_companies}, Updated: {updated_companies}, "
                f"Failed: {failed}"
            )
            await asyncio.sleep(batch_delay)

        self.logger.info(
            f"[{self.country_code}] Discovery complete: "
            f"{new_companies} new companies, "
            f"{updated_companies} updated, "
            f"{failed} failed"
        )
        return processed

    async def _extract_company_info(self, page: Page, url: str, advertiser_name: str) -> dict:
        """Extract company info from an ad detail page."""
        try:
            # Block unnecessary resources to speed up loading
            await page.route("**/*", lambda route: self._filter_requests(route))

            response = await page.goto(
                url,
                wait_until="domcontentloaded",
                timeout=NAVIGATION_TIMEOUT
            )

            if not response or not response.ok:
                self.logger.warning(f"HTTP {response.status if response else 'None'} for {url}")
                return None

            # Wait for content to render (don't use networkidle — it times out)
            await asyncio.sleep(2)

            html_content = await page.content()
            info = {'advertiser_name': advertiser_name}

            # Company ID from numeric company link
            company_match = re.search(
                r'<a[^>]*href="https://www\.linkedin\.com/company/(\d+)[^"]*"[^>]*>',
                html_content
            )
            if company_match:
                info['company_id'] = int(company_match.group(1))
                info['company_url'] = f"https://www.linkedin.com/company/{company_match.group(1)}"

            # Company URL from slug-based link (fallback)
            if 'company_url' not in info:
                slug_match = re.search(
                    r'href="(https://www\.linkedin\.com/company/[^"]+)"',
                    html_content
                )
                if slug_match:
                    info['company_url'] = slug_match.group(1)

            # Personal profile URL
            profile_match = re.search(
                r'href="(https://www\.linkedin\.com/in/[^"]+)"',
                html_content
            )
            if profile_match:
                info['profile_url'] = profile_match.group(1)

            # Ad type
            info['ad_type'] = 'personal_ad' if 'linkedin.com/in/' in html_content else 'company_ad'

            # Promoted by: link with company ID
            promoted_match = re.search(
                r'Promoted\s+by\s+<a[^>]*href="https://www\.linkedin\.com/company/(\d+)[^"]*"[^>]*>\s*([^<]+)\s*</a>',
                html_content
            )
            if promoted_match:
                info['promoted_by_company_id'] = int(promoted_match.group(1))
                info['promoted_by_name'] = promoted_match.group(2).strip()
            else:
                # Fallback: plain text
                text_match = re.search(r'Promoted\s+by\s+([^<\n]+)', html_content)
                if text_match:
                    info['promoted_by_name'] = text_match.group(1).strip()

            info['first_seen_country'] = self.country_code
            return info

        except Exception as e:
            self.logger.error(f"Extraction failed for {url}: {str(e)}")
            return None


    async def _filter_requests(self, route):
        """Block non-essential resources to speed up page loads."""
        request = route.request
        resource_type = request.resource_type
        url = request.url.lower()

        if resource_type == "document" or "ad-library" in url:
            await route.continue_()
            return

        if resource_type in ["media", "video", "font", "image"]:
            await route.abort()
            return

        if resource_type in ["script", "stylesheet"] and not any(
            essential in url for essential in ["ad-library", "essential", "core"]
        ):
            await route.abort()
            return

        if any(p in url for p in ["analytics", "tracking", "metrics", "telemetry", "pixel", "beacon"]):
            await route.abort()
            return

        await route.continue_()


async def run_parallel_discovery(
    country_codes: list,
    max_parallel: int = 3,
    playwright=None
) -> dict:
    """Run discovery across multiple countries in parallel.

    Each country gets its own SBR connection for scrolling.
    Returns {country: stats_dict}.
    """
    semaphore = asyncio.Semaphore(max_parallel)
    results = {}

    async def discover_country(country_code: str):
        async with semaphore:
            crawler = CompanyDiscoveryCrawler(country_code)
            sbr_browser = None
            try:
                async with AsyncSessionLocal() as db:
                    sbr_browser, context, page = await create_fresh_sbr_connection(playwright)

                    # Phase 1: Scroll and identify unknowns
                    unknown = await crawler.discover_from_listing(page, db)

                    # Close the scrolling browser
                    await sbr_browser.close()
                    sbr_browser = None

                    # Phase 2: Process unknown advertisers
                    processed = await crawler.process_unknown_advertisers(db, playwright)

                    results[country_code] = {
                        'cards_seen': crawler.total_cards_seen,
                        'unknown_found': len(unknown),
                        'known_skipped': crawler.known_count,
                        'processed': processed,
                    }
            except Exception as e:
                logger.error(f"[{country_code}] Discovery failed: {str(e)}")
                results[country_code] = {'error': str(e)}
            finally:
                if sbr_browser:
                    try:
                        await sbr_browser.close()
                    except Exception:
                        pass

    tasks = [discover_country(cc) for cc in country_codes]
    await asyncio.gather(*tasks, return_exceptions=True)

    # Summary
    total_new = sum(r.get('processed', 0) for r in results.values() if 'error' not in r)
    total_skipped = sum(r.get('known_skipped', 0) for r in results.values() if 'error' not in r)
    logger.info(
        f"Discovery complete across {len(country_codes)} countries: "
        f"{total_new} new companies processed, {total_skipped} known skipped"
    )

    return results
