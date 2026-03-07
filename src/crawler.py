from playwright.async_api import Page
import logging
import asyncio
from .config import (
    crawler_config,
    browser_config,
    brightdata_config,
    MAX_CONCURRENT_PAGES,
    VIEWPORT_CONFIG,
    RETRY_COUNT,
    RETRY_DELAY,
    NAVIGATION_TIMEOUT,
    CHUNK_SIZE,
    get_random_user_agent,
)
from .utils import clean_text, clean_percentage, format_date, create_new_context_with_proxy, create_fresh_sbr_connection
from src.logger import setup_logger
import re
from datetime import datetime, date
import time
from src.models import LinkedInAd
from src.utils import generate_linkedin_url
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
import json

logger = setup_logger("crawler", "DEBUG")


class AsyncLinkedInCrawler:
    def __init__(self, company_id: str, max_requests: int = 50):
        self.company_id = company_id
        self.max_requests = max_requests
        self.detail_urls = set()
        self.logger = setup_logger("crawler")
        self.consecutive_timeouts = 0
        self.max_consecutive_timeouts = 3
        self.metrics = {
            'start_time': None,
            'url_collection_time': None,
            'processing_times': [],
            'failed_urls': set(),
            'success_rate': 0,
            'avg_processing_time': 0
        }
        self.active_contexts = []
        self.active_pages = []
        self._rate_limit_hits = 0

    async def collect_ad_urls(self, page: Page) -> None:
        """Collect all ad URLs by scrolling the listing page."""
        self.metrics['start_time'] = time.time()
        self.logger.info("Starting URL collection")

        url = generate_linkedin_url(self.company_id)
        self.logger.info(f"Navigating to {url}")

        try:
            await page.goto(
                url,
                wait_until='domcontentloaded',
                timeout=browser_config.NAVIGATION_TIMEOUT
            )
            await asyncio.sleep(crawler_config.INITIAL_WAIT_TIME)

            previous_links_count = 0
            consecutive_unchanged_counts = 0
            scroll_count = 0

            while True:
                self.logger.debug(f"Scroll iteration {scroll_count}")

                try:
                    await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')

                    wait_time = crawler_config.BASE_SCROLL_WAIT + (consecutive_unchanged_counts * crawler_config.SCROLL_INCREMENT)
                    await asyncio.sleep(wait_time)

                    links = await page.eval_on_selector_all(
                        "a[href*='/ad-library/detail/']",
                        "elements => Array.from(elements).map(el => el.href)"
                    )

                    current_links = set(link.split('?')[0] for link in links)
                    new_links = len(current_links) - previous_links_count

                    if new_links > 0:
                        self.logger.info(f"Found {new_links} new URLs. Total: {len(current_links)}")
                        consecutive_unchanged_counts = 0
                    else:
                        consecutive_unchanged_counts += 1

                    self.detail_urls.update(current_links)
                    previous_links_count = len(current_links)

                except Exception as e:
                    self.logger.error(f"Error during scroll: {str(e)}")
                    consecutive_unchanged_counts += 1

                scroll_count += 1

                if consecutive_unchanged_counts >= crawler_config.MAX_UNCHANGED_SCROLLS:
                    try:
                        await page.evaluate('window.scrollTo(0, 0)')
                        await asyncio.sleep(crawler_config.SCROLL_WAIT_TIME)
                        await page.evaluate('window.scrollTo(0, document.body.scrollHeight)')
                        await asyncio.sleep(crawler_config.SCROLL_WAIT_TIME)

                        final_links = await page.eval_on_selector_all(
                            "a[href*='/ad-library/detail/']",
                            "elements => Array.from(elements).map(el => el.href)"
                        )
                        final_set = set(link.split('?')[0] for link in final_links)

                        if len(final_set) > len(self.detail_urls):
                            self.logger.info(f"Found {len(final_set) - len(self.detail_urls)} additional URLs in final check")
                            self.detail_urls.update(final_set)
                            consecutive_unchanged_counts = 0
                            continue

                        self.logger.info("No new URLs found in final check, proceeding to process ads")
                        break
                    except Exception as e:
                        self.logger.error(f"Error during final check: {str(e)}")
                        break

                if scroll_count > crawler_config.MAX_SCROLL_COUNT:
                    self.logger.warning("Reached maximum scroll count")
                    break

            self.logger.info(f"URL collection complete. Found {len(self.detail_urls)} unique URLs")
            self.metrics['url_collection_time'] = time.time() - self.metrics['start_time']

        except Exception as e:
            self.logger.error(f"Failed to collect URLs: {str(e)}")

    async def process_all_ads(self, page: Page, db: AsyncSession, playwright=None) -> int:
        """Process all collected ad URLs in batches for optimal throughput."""
        processing_start = time.time()
        total_ads = len(self.detail_urls)

        if total_ads == 0:
            self.logger.warning("No ads found to process")
            return 0

        batch_size = MAX_CONCURRENT_PAGES
        use_sbr = brightdata_config.has_scraping_browser() and playwright is not None
        self.logger.info(
            f"Starting BATCH processing of {total_ads} ads "
            f"(batch size: {batch_size}, mode: {'fresh_sbr' if use_sbr else 'shared_browser'})"
        )

        self._new_ads = 0
        self._updated_ads = 0
        self._existing_ads = 0
        self._processed_count = 0
        self._rate_limit_hits = 0
        self._batch_rate_limits = 0
        self._db_lock = asyncio.Lock()
        self._playwright = playwright

        browser = page.context.browser
        urls = list(self.detail_urls)
        batch_delay = 3  # seconds between batches

        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(urls) + batch_size - 1) // batch_size

            self._batch_rate_limits = 0
            tasks = [self._process_single_ad(url, browser, db, total_ads) for url in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Log progress
            pct = (self._processed_count / total_ads) * 100
            self.logger.info(
                f"Batch {batch_num}/{total_batches} done — "
                f"Progress: {self._processed_count}/{total_ads} ({pct:.1f}%) "
                f"— New: {self._new_ads} Updated: {self._updated_ads} "
                f"Existing: {self._existing_ads} RateLimits: {self._rate_limit_hits}"
            )

            # Adaptive delay: increase pause if this batch had rate limits
            if self._batch_rate_limits > 0:
                batch_delay = min(batch_delay + 5, 30)
                self.logger.info(f"Rate limits detected, increasing batch delay to {batch_delay}s")
            elif batch_delay > 3:
                batch_delay = max(batch_delay - 1, 3)

            await asyncio.sleep(batch_delay)

        total_time = time.time() - processing_start
        processed_count = self._new_ads + self._updated_ads + self._existing_ads

        self.logger.info(
            f"\nPerformance Metrics:\n"
            f"- Total Processing Time: {total_time:.2f}s\n"
            f"- Batch Size: {batch_size}\n"
            f"- Ads/second: {processed_count / max(total_time, 1):.1f}\n"
            f"- Failed URLs: {len(self.metrics['failed_urls'])}\n"
            f"- Rate Limit Hits: {self._rate_limit_hits}\n"
            f"- New Ads: {self._new_ads}\n"
            f"- Updated Ads: {self._updated_ads}\n"
            f"- Existing Ads: {self._existing_ads}"
        )

        return processed_count

    async def _process_single_ad(self, url: str, browser, db: AsyncSession, total: int):
        """Process a single ad URL. Uses fresh SBR connection if available, else shared browser."""
        sbr_browser = None
        context = None
        try:
            if brightdata_config.has_scraping_browser() and self._playwright:
                # Fresh CDP connection = fresh IP per worker
                sbr_browser, context, page = await create_fresh_sbr_connection(self._playwright)
            elif brightdata_config.has_residential_proxy():
                context = await create_new_context_with_proxy(browser)
                page = await context.new_page()
            else:
                context = await browser.new_context(
                    viewport=VIEWPORT_CONFIG,
                    user_agent=get_random_user_agent()
                )
                page = await context.new_page()

            ad_details = await self.extract_ad_details(page, url)

            if ad_details and ad_details != "RATE_LIMITED":
                async with self._db_lock:
                    status = await self.upsert_ad(db, ad_details)
                    if status == 'new':
                        self._new_ads += 1
                    elif status == 'updated':
                        self._updated_ads += 1
                    elif status == 'existing':
                        self._existing_ads += 1
                    self._processed_count += 1

            elif ad_details == "RATE_LIMITED":
                self._rate_limit_hits += 1
                self._batch_rate_limits += 1
                self.metrics['failed_urls'].add(url)

        except Exception as e:
            self.logger.error(f"Worker failed for {url}: {str(e)}")
            self.metrics['failed_urls'].add(url)
        finally:
            # Close SBR browser connection (not just context)
            if sbr_browser:
                try:
                    await sbr_browser.close()
                except Exception:
                    pass
            elif context:
                try:
                    await context.close()
                except Exception:
                    pass

    async def extract_ad_details(self, page: Page, url: str) -> dict:
        """Extract details from a single ad page with retry logic."""
        for attempt in range(RETRY_COUNT):
            try:
                await page.route("**/*", lambda route: self._filter_requests(route))

                response = await page.goto(
                    url,
                    wait_until="domcontentloaded",
                    timeout=NAVIGATION_TIMEOUT
                )

                if response.status == 429:
                    self.logger.warning(f"Rate limited (429) on {url}")
                    return "RATE_LIMITED"

                if not response.ok:
                    self.logger.error(f"HTTP {response.status} on attempt {attempt + 1}")
                    continue

                await asyncio.sleep(1)
                return await self._extract_page_content(page)

            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt < RETRY_COUNT - 1:
                    wait_time = 5 * (attempt + 1)
                    await asyncio.sleep(wait_time)
                    continue
                return None

        return None

    async def _extract_demographics(self, page: Page) -> dict:
        demographics = {}
        try:
            demo_elements = await page.query_selector_all('.ad-library-preview-demographic-data')
            for element in demo_elements:
                try:
                    label = await element.eval_on_selector('.demographic-data-label', 'el => el.textContent')
                    value = await element.eval_on_selector('.demographic-data-value', 'el => el.textContent')
                    if 'gender' in label.lower():
                        demographics['gender'] = clean_percentage(value)
                    elif 'age' in label.lower():
                        demographics['age'] = clean_percentage(value)
                    elif 'seniority' in label.lower():
                        demographics['seniority'] = clean_percentage(value)
                except Exception:
                    continue
        except Exception as e:
            self.logger.error(f"Error extracting demographics: {str(e)}")
        return demographics

    async def _extract_page_content(self, page: Page) -> dict:
        try:
            await page.wait_for_load_state('networkidle')
            ad_data = {}

            html_content = await page.content()

            ad_id_match = re.search(r'<link rel="canonical" href="/ad-library/detail/(\d+)">', html_content)
            if not ad_id_match:
                self.logger.warning('Ad ID not found, skipping ad.')
                return None
            ad_data['ad_id'] = ad_id_match.group(1)

            duration_match = re.search(r'<p class="about-ad__availability-duration[^"]*"[^>]*>([^<]*)</p>', html_content)
            if duration_match:
                duration_text = duration_match.group(1).strip()
                date_range = re.search(r'Ran from (\w+ \d{1,2}, \d{4})(?:\s+to\s+(\w+ \d{1,2}, \d{4}))?', duration_text)
                if date_range:
                    ad_data['campaign_start_date'] = format_date(date_range.group(1))
                    ad_data['campaign_end_date'] = format_date(date_range.group(2)) if date_range.group(2) else None

            impressions_match = re.search(r'<p[^>]*>Total Impressions</p>\s*<p[^>]*>([^<]*)</p>', html_content)
            if impressions_match:
                ad_data['campaign_impressions_range'] = impressions_match.group(1).strip()

            country_impressions = []
            country_pattern = r'<span class="ad-analytics__country-impressions[^"]*"[^>]*aria-label="([^"]+), impressions ([^%]+%)"[^>]*>'
            for match in re.finditer(country_pattern, html_content):
                country_impressions.append({
                    'country': match.group(1),
                    'percentage': match.group(2)
                })
            ad_data['campaign_impressions_by_country'] = country_impressions

            logo_match = re.search(r'<img[^>]*data-delayed-url="([^"]+)"[^>]*alt="advertiser logo"[^>]*>', html_content)
            if logo_match:
                ad_data['advertiser_logo'] = logo_match.group(1)

            advertiser_match = re.search(r'<a[^>]*href="https://www\.linkedin\.com/(?:company|in)/[^"]+"[^>]*>\s*([^<]+)\s*</a>', html_content)
            if advertiser_match:
                ad_data['advertiser_name'] = advertiser_match.group(1).strip()

            creative_match = re.search(r'<div[^>]*data-creative-type="([^"]+)"[^>]*>', html_content)
            if creative_match:
                ad_data['creative_type'] = creative_match.group(1)

            ad_data['ad_type'] = 'personal_ad' if 'linkedin.com/in/' in html_content else 'company_ad'

            # Extract "promoted by" info for personal ads
            promoted_by_match = re.search(
                r'Promoted\s+by\s+<a[^>]*href="https://www\.linkedin\.com/company/(\d+)[^"]*"[^>]*>\s*([^<]+)\s*</a>',
                html_content
            )
            if promoted_by_match:
                ad_data['promoted_text'] = f"Promoted by {promoted_by_match.group(2).strip()}"
            else:
                # Alternative: plain text "Promoted by CompanyName"
                promoted_text_match = re.search(r'Promoted\s+by\s+([^<\n]+)', html_content)
                if promoted_text_match:
                    ad_data['promoted_text'] = f"Promoted by {promoted_text_match.group(1).strip()}"

            redirect_match = re.search(r'<a[^>]*href="([^"]+)"[^>]*data-tracking-control-name="ad_library_ad_preview_headline_content"[^>]*>', html_content)
            if redirect_match:
                full_url = redirect_match.group(1)
                url_parts = full_url.split('?')
                ad_data['ad_redirect_url'] = url_parts[0]
                ad_data['utm_parameters'] = url_parts[1] if len(url_parts) > 1 else None

            headline_match = re.search(r'<h1[^>]*class="headline"[^>]*>([^<]+)</h1>', html_content)
            if headline_match:
                ad_data['headline'] = headline_match.group(1).strip()

            description_match = re.search(r'<p[^>]*class="[^"]*commentary__content[^"]*"[^>]*>([\s\S]*?)</p>', html_content)
            if description_match:
                ad_data['description'] = clean_text(description_match.group(1).strip())
            else:
                ad_data['description'] = None

            img_match = re.search(r'<img[^>]*class="[^"]*ad-preview__dynamic-dimensions-image[^"]*"[^>]*src="([^"]+)"', html_content)
            if img_match:
                ad_data['image_url'] = img_match.group(1).replace('&amp;', '&')

            company_match = re.search(r'<a[^>]*href="https://www\.linkedin\.com/company/(\d+)"[^>]*>', html_content)
            if company_match:
                ad_data['company_id'] = company_match.group(1)

            # Use initialized company_id as fallback
            if 'company_id' not in ad_data or not ad_data['company_id']:
                try:
                    ad_data['company_id'] = int(self.company_id)
                except (ValueError, TypeError):
                    ad_data['company_id'] = None
            else:
                try:
                    ad_data['company_id'] = int(ad_data['company_id'])
                except (ValueError, TypeError):
                    ad_data['company_id'] = None

            return ad_data

        except Exception as e:
            self.logger.error(f"Error in content extraction: {str(e)}")
            return None

    async def _filter_requests(self, route):
        request = route.request
        resource_type = request.resource_type
        url = request.url.lower()

        if resource_type == "document" or "ad-library" in url:
            await route.continue_()
            return

        if resource_type in ["media", "video", "font"]:
            await route.abort()
            return

        if resource_type == "image" and any(ext in url for ext in ['.gif', '.webp']):
            await route.abort()
            return

        if resource_type in ["script", "stylesheet"] and not any(
            essential in url for essential in ["ad-library", "essential", "core"]
        ):
            await route.abort()
            return

        if any(pattern in url for pattern in [
            "analytics", "tracking", "metrics", "telemetry",
            "logging", "pixel", "beacon"
        ]):
            await route.abort()
            return

        await route.continue_()

    async def upsert_ad(self, db: AsyncSession, ad_data: dict):
        try:
            if not ad_data.get('ad_id'):
                raise ValueError("ad_id is required")

            transformed_data = self._transform_ad_data(ad_data)

            existing_ad = await db.execute(
                select(LinkedInAd).where(LinkedInAd.ad_id == transformed_data['ad_id'])
            )
            existing_ad = existing_ad.scalars().first()

            if existing_ad:
                needs_update = False
                for field, new_value in transformed_data.items():
                    if field == 'ad_id':
                        continue
                    current_value = getattr(existing_ad, field)

                    if isinstance(current_value, (date, datetime)):
                        if new_value and current_value != new_value:
                            setattr(existing_ad, field, new_value)
                            needs_update = True
                    elif isinstance(current_value, (dict, list)):
                        if new_value and json.dumps(current_value) != json.dumps(new_value):
                            setattr(existing_ad, field, new_value)
                            needs_update = True
                    elif current_value != new_value:
                        setattr(existing_ad, field, new_value)
                        needs_update = True

                if needs_update:
                    await db.commit()
                    return 'updated'
                return 'existing'
            else:
                new_ad = LinkedInAd(**transformed_data)
                db.add(new_ad)
                await db.commit()
                return 'new'

        except Exception as e:
            await db.rollback()
            self.logger.error(f"Error in upsert_ad: {str(e)}")
            raise

    def _transform_ad_data(self, data: dict) -> dict:
        transformed = {}

        for field in [
            'ad_id', 'creative_type', 'advertiser_name', 'advertiser_logo',
            'headline', 'description', 'promoted_text', 'image_url',
            'view_details_link', 'campaign_impressions_range',
            'company_id', 'ad_type', 'ad_redirect_url', 'utm_parameters'
        ]:
            if field in data:
                transformed[field] = data[field]

        for date_field in ['campaign_start_date', 'campaign_end_date']:
            if data.get(date_field):
                try:
                    transformed[date_field] = datetime.strptime(
                        data[date_field], '%Y/%m/%d'
                    ).date()
                except ValueError:
                    transformed[date_field] = None

        if 'campaign_impressions_by_country' in data:
            transformed['campaign_impressions_by_country'] = (
                data['campaign_impressions_by_country']
                if isinstance(data['campaign_impressions_by_country'], (dict, list))
                else json.loads(data['campaign_impressions_by_country'])
            )

        if 'company_id' in transformed:
            try:
                transformed['company_id'] = int(transformed['company_id'])
            except (ValueError, TypeError):
                transformed['company_id'] = None

        return transformed

    async def cleanup(self):
        for context in self.active_contexts:
            try:
                await context.close()
            except Exception as e:
                self.logger.error(f"Error closing context: {e}")
        self.active_contexts = []
        self.active_pages = []
