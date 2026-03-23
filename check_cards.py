#!/usr/bin/env python3
"""Quick test to see what company data is visible on LinkedIn Ad Library listing cards."""
import asyncio
from playwright.async_api import async_playwright
from src.config import brightdata_config

async def check_listing_cards():
    async with async_playwright() as pw:
        browser = await pw.chromium.connect_over_cdp(brightdata_config.SBR_WS_ENDPOINT)
        context = browser.contexts[0] if browser.contexts else await browser.new_context()
        page = await context.new_page()

        print("Navigating to US ad library...")
        await page.goto(
            "https://www.linkedin.com/ad-library/search?countries=US",
            wait_until="domcontentloaded",
        )
        await asyncio.sleep(5)

        # Extract ALL company info from ad cards
        results = await page.evaluate("""() => {
            const cards = document.querySelectorAll('.search-result-item');
            const companies = [];

            cards.forEach(card => {
                const ariaLabel = card.querySelector('[aria-label]');
                const companyLink = card.querySelector('a[href*="/company/"]');
                const personLink = card.querySelector('a[href*="/in/"]');
                const detailLink = card.querySelector('a[href*="/ad-library/detail/"]');
                const advertiserImg = card.querySelector('img[alt*="logo"], img[alt*="advertiser"]');

                companies.push({
                    ariaLabel: ariaLabel ? ariaLabel.getAttribute('aria-label').substring(0, 200) : null,
                    companyHref: companyLink ? companyLink.href : null,
                    companyText: companyLink ? companyLink.textContent.trim() : null,
                    personHref: personLink ? personLink.href : null,
                    personText: personLink ? personLink.textContent.trim() : null,
                    detailHref: detailLink ? detailLink.href : null,
                    imgAlt: advertiserImg ? advertiserImg.alt : null,
                });
            });

            return { total: cards.length, companies: companies };
        }""")

        print(f"\nTotal ad cards: {results['total']}")
        print(f"\nFirst 10 cards:")
        for i, c in enumerate(results['companies'][:10]):
            print(f"\n--- Card {i+1} ---")
            print(f"  aria-label: {c['ariaLabel']}")
            print(f"  company:    {c['companyText']} -> {c['companyHref']}")
            print(f"  person:     {c['personText']} -> {c['personHref']}")
            print(f"  detail:     {c['detailHref']}")

        # Count how many have company links vs not
        with_company = sum(1 for c in results['companies'] if c['companyHref'])
        with_person = sum(1 for c in results['companies'] if c['personHref'])
        with_aria = sum(1 for c in results['companies'] if c['ariaLabel'])
        print(f"\n--- Summary ---")
        print(f"Cards with company link: {with_company}/{results['total']}")
        print(f"Cards with person link:  {with_person}/{results['total']}")
        print(f"Cards with aria-label:   {with_aria}/{results['total']}")

        await browser.close()

asyncio.run(check_listing_cards())
