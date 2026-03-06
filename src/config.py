import os
import logging
import random
from typing import Optional
from pydantic_settings import BaseSettings
from functools import lru_cache
from dotenv import load_dotenv

load_dotenv()

# Rotating user agents for anti-detection
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


class BrowserConfig:
    USER_AGENT = get_random_user_agent()
    VIEWPORT = {'width': 1280, 'height': 800}
    PAGE_TIMEOUT = 60000
    NAVIGATION_TIMEOUT = 60000
    MAX_CONCURRENT_PAGES = 2
    CHUNK_SIZE = 2
    RETRY_COUNT = 5
    RETRY_DELAY = 5


class CrawlerConfig:
    MAX_REQUESTS = 50
    SCROLL_TIMEOUT = 3
    SCROLL_COUNT = {'MIN': 3, 'MAX': 500}
    SCROLL_WAIT_TIME = 2
    NETWORK_IDLE_TIMEOUT = 5000
    MAX_CONSECUTIVE_UNCHANGED = 5
    INITIAL_WAIT_TIME = 5
    BASE_SCROLL_WAIT = 3
    SCROLL_INCREMENT = 1
    MAX_UNCHANGED_SCROLLS = 5
    MAX_SCROLL_COUNT = 20
    CHUNK_SIZE = 5
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    PARALLEL_CHUNKS = 2


class LogConfig:
    FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
    LEVEL = logging.INFO
    DEBUG = logging.DEBUG


class ProxyConfig:
    """BrightData proxy configuration.

    Set these in your .env file:
        BRIGHTDATA_HOST=brd.superproxy.io
        BRIGHTDATA_PORT=22225
        BRIGHTDATA_USERNAME=brd-customer-XXXXX-zone-XXXXX
        BRIGHTDATA_PASSWORD=XXXXX
    """
    HOST = os.getenv("BRIGHTDATA_HOST", "brd.superproxy.io")
    PORT = int(os.getenv("BRIGHTDATA_PORT", "22225"))
    USERNAME = os.getenv("BRIGHTDATA_USERNAME", "")
    PASSWORD = os.getenv("BRIGHTDATA_PASSWORD", "")

    @classmethod
    def is_configured(cls) -> bool:
        return bool(cls.USERNAME and cls.PASSWORD)

    @classmethod
    def get_proxy_url(cls) -> Optional[str]:
        if not cls.is_configured():
            return None
        return f"http://{cls.USERNAME}:{cls.PASSWORD}@{cls.HOST}:{cls.PORT}"

    @classmethod
    def get_playwright_proxy(cls) -> Optional[dict]:
        if not cls.is_configured():
            return None
        return {
            "server": f"http://{cls.HOST}:{cls.PORT}",
            "username": cls.USERNAME,
            "password": cls.PASSWORD,
        }


class Settings(BaseSettings):
    DB_PATH: str = "linkedin_ads.db"
    ENVIRONMENT: str = "development"
    BRIGHTDATA_HOST: str = "brd.superproxy.io"
    BRIGHTDATA_PORT: str = "22225"
    BRIGHTDATA_USERNAME: str = ""
    BRIGHTDATA_PASSWORD: str = ""

    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


browser_config = BrowserConfig()
crawler_config = CrawlerConfig()
log_config = LogConfig()
proxy_config = ProxyConfig()

MAX_CONCURRENT_PAGES = browser_config.MAX_CONCURRENT_PAGES
VIEWPORT_CONFIG = browser_config.VIEWPORT
USER_AGENT = browser_config.USER_AGENT
NAVIGATION_TIMEOUT = browser_config.NAVIGATION_TIMEOUT
RETRY_COUNT = browser_config.RETRY_COUNT
RETRY_DELAY = browser_config.RETRY_DELAY
CHUNK_SIZE = browser_config.CHUNK_SIZE
