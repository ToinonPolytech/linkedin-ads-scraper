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
from .utils import clean_text, clean_percentage, format_date, create_new_context_with_proxy
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

    async def process_all_ads(self, page: Page, db: AsyncSession) -> int:
        """Process all collected ad URLs with proxy rotation on rate limits."""
        processing_start = time.time()
        total_ads = len(self.detail_urls)

        if total_ads == 0:
            self.logger.warning("No ads found to process")
            return 0

        start_time = datetime.now()
        new_ads = 0
        updated_ads = 0
        existing_ads = 0

        self.logger.info(f"Starting processing of {total_ads} ads")

        try:
            browser = page.context.browser
            contexts = []
            pages = []

            for i in range(MAX_CONCURRENT_PAGES):
                if brightdata_config.is_configured():
                    context = await create_new_context_with_proxy(browser)
                else:
                    context = await browser.new_context(
                        viewport=VIEWPORT_CONFIG,
                        user_agent=get_random_user_agent()
                    )
                contexts.append(context)
                pages.append(await context.new_page())

            new_ads, updated_ads, existing_ads = await self.process_urls(
                list(self.detail_urls), pages, db, browser
            )

            total_time = time.time() - processing_start
            if total_ads > 0:
                self.metrics['success_rate'] = ((total_ads - len(self.metrics['failed_urls'])) / total_ads) * 100
                if self.metrics['processing_times']:
                    self.metrics['avg_processing_time'] = sum(self.metrics['processing_times']) / len(self.metrics['processing_times'])

            self.logger.info(
                f"\nPerformance Metrics:\n"
                f"- Total Processing Time: {total_time:.2f}s\n"
                f"- Average Processing Time: {self.metrics['avg_processing_time']:.2f}s\n"
                f"- Success Rate: {self.metrics['success_rate']:.1f}%\n"
                f"- Failed URLs: {len(self.metrics['failed_urls'])}\n"
                f"- Rate Limit Hits: {self._rate_limit_hits}\n"
                f"- New Ads: {new_ads}\n"
                f"- Updated Ads: {updated_ads}\n"
                f"- Existing Ads: {existing_ads}"
            )

        finally:
            for context in contexts:
                try:
                    await context.close()
                except Exception:
                    pass

        processed_count = new_ads + updated_ads + existing_ads
        return processed_count

    async def _rotate_proxy(self, browser, page_index: int, pages: list, contexts: list):
        """Rotate to a new proxy by creating a fresh context (new BrightData session = new IP)."""
        self.logger.info(f"Rotating proxy for page index {page_index}")
        try:
            old_context = contexts[page_index]
            await old_context.close()
        except Exception:
            pass

        if brightdata_config.is_configured():
            new_context = await create_new_context_with_proxy(browser)
        else:
            new_context = await browser.new_context(
                viewport=VIEWPORT_CONFIG,
                user_agent=get_random_user_agent()
            )

        contexts[page_index] = new_context
        pages[page_index] = await new_context.new_page()
        self.logger.info(f"Proxy rotated — new user-agent and IP for page {page_index}")

    async def process_urls(self, urls: list, pages: list, db: AsyncSession, browser=None) -> tuple:
        """Process URLs with automatic proxy rotation on rate limits."""
        new_ads = updated_ads = existing_ads = 0
        total_urls = len(urls)
        processed_count = 0
        consecutive_existing_count = 0

        # Keep track of contexts for rotation
        contexts = [p.context for p in pages]

        for i, url in enumerate(urls):
            page_index = i % len(pages)
            try:
                ad_details = await self.extract_ad_details(pages[page_index], url)

                # Handle rate limit — rotate proxy and retry
                if ad_details == "RATE_LIMITED" and browser and brightdata_config.is_configured():
                    self._rate_limit_hits += 1
                    self.logger.warning(f"Rate limited — rotating proxy (hit #{self._rate_limit_hits})")
                    await self._rotate_proxy(browser, page_index, pages, contexts)
                    await asyncio.sleep(5)
                    ad_details = await self.extract_ad_details(pages[page_index], url)

                if ad_details and ad_details != "RATE_LIMITED":
                    status = await self.upsert_ad(db, ad_details)
                    if status == 'new':
                        new_ads += 1
                        consecutive_existing_count = 0
                    elif status == 'updated':
                        updated_ads += 1
                        consecutive_existing_count = 0
                    elif status == 'existing':
                        existing_ads += 1
                        consecutive_existing_count += 1

                    if consecutive_existing_count > 30:
                        self.logger.info("Aborting — 30+ consecutive existing ads")
                        break

                processed_count += 1
                if processed_count % 10 == 0:
                    self.logger.info(f"Progress: {processed_count}/{total_urls} ({(processed_count/total_urls)*100:.1f}%)")

            except Exception as e:
                self.logger.error(f"Failed to process URL {url}: {str(e)}")
                self.metrics['failed_urls'].add(url)

        self.logger.info(f"Results — New: {new_ads}, Updated: {updated_ads}, Existing: {existing_ads}")
        return new_ads, updated_ads, existing_ads

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
