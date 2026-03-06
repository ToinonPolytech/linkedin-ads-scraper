from datetime import datetime
import re
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import asyncio
from .models import LinkedInAd
from .database import AsyncSessionLocal, engine, Base
from .config import (
    VIEWPORT_CONFIG, NAVIGATION_TIMEOUT,
    get_random_user_agent, brightdata_config,
)
import time
import logging

logger = logging.getLogger(__name__)


async def init_db():
    from src.models import LinkedInAd
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    logger.info("Database tables created successfully")


async def close_db():
    await engine.dispose()


def clean_text(text_str: str) -> str:
    if not text_str:
        return ""
    text_str = re.sub(r'<[^>]+>', '', text_str)
    text_str = re.sub(r'\s+', ' ', text_str)
    return text_str.strip()


def clean_percentage(value: str) -> str:
    if not value:
        return "0%"
    value = value.lower()
    if "less than" in value:
        return "<1%"
    return value.strip()


def format_date(date_str: str) -> str:
    if not date_str:
        return None
    try:
        date_obj = datetime.strptime(date_str.strip(), '%b %d, %Y')
        return date_obj.strftime('%Y/%m/%d')
    except Exception:
        return None


def extract_with_regex(pattern, html, group=1):
    match = re.search(pattern, html)
    return match.group(group).strip() if match else None


def generate_linkedin_url(company_id: str) -> str:
    return (f"https://www.linkedin.com/ad-library/search?companyIds={company_id}"
            if company_id.isdigit()
            else f"https://www.linkedin.com/ad-library/search?accountOwner={company_id}")


async def setup_browser_context(playwright):
    """Set up browser — uses BrightData Scraping Browser if configured, otherwise local."""
    mode = brightdata_config.get_mode()

    # ── Option 1: BrightData Scraping Browser (cloud browser, auto proxy rotation) ──
    if mode == "scraping_browser":
        endpoint = brightdata_config.SBR_WS_ENDPOINT
        logger.info(f"Connecting to BrightData Scraping Browser...")
        browser = await playwright.chromium.connect_over_cdp(endpoint)
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        logger.info("Connected to Scraping Browser (cloud browser with auto proxy + anti-detect)")
        return browser, context

    # ── Option 2: Local browser + BrightData residential proxy ──
    if mode == "residential_proxy":
        proxy = brightdata_config.get_playwright_proxy()
        logger.info(f"Using BrightData residential proxy: {brightdata_config.HOST}:{brightdata_config.PORT}")
    else:
        proxy = None
        logger.warning("No BrightData configured — running without proxy rotation")

    user_agent = get_random_user_agent()

    browser = await playwright.chromium.launch(
        headless=True,
        args=[
            '--disable-gpu',
            '--disable-dev-shm-usage',
            '--disable-setuid-sandbox',
            '--no-sandbox',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
        ]
    )

    context = await browser.new_context(
        viewport=VIEWPORT_CONFIG,
        user_agent=user_agent,
        proxy=proxy,
        java_script_enabled=True,
        bypass_csp=True,
        ignore_https_errors=True,
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    )

    await context.route("**/*.{css,font,woff,woff2}",
        lambda route: route.abort())

    context.set_default_timeout(NAVIGATION_TIMEOUT)
    context.set_default_navigation_timeout(NAVIGATION_TIMEOUT)

    return browser, context


async def create_new_context_with_proxy(browser):
    """Create a fresh context for proxy rotation.

    - Scraping Browser: reconnect for new IP
    - Residential proxy: new context = new session = new IP
    """
    mode = brightdata_config.get_mode()

    if mode == "scraping_browser":
        # Scraping Browser auto-rotates; new context suffices
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        return context

    proxy = brightdata_config.get_playwright_proxy()
    user_agent = get_random_user_agent()

    context = await browser.new_context(
        viewport=VIEWPORT_CONFIG,
        user_agent=user_agent,
        proxy=proxy,
        java_script_enabled=True,
        bypass_csp=True,
        ignore_https_errors=True,
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    )

    await context.route("**/*.{css,font,woff,woff2}",
        lambda route: route.abort())

    context.set_default_timeout(NAVIGATION_TIMEOUT)
    context.set_default_navigation_timeout(NAVIGATION_TIMEOUT)

    return context


async def batch_upsert_ads(ads: list, db: AsyncSession, batch_size: int = 100):
    for i in range(0, len(ads), batch_size):
        batch = ads[i:i + batch_size]
        ad_objects = [LinkedInAd(**ad) for ad in batch]
        db.add_all(ad_objects)
        await asyncio.sleep(0.1)
    await db.commit()


class CrawlerMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.successful_requests = 0
        self.failed_requests = 0
        self.total_processing_time = 0

    def get_success_rate(self):
        total = self.successful_requests + self.failed_requests
        return (self.successful_requests / total * 100) if total > 0 else 0
