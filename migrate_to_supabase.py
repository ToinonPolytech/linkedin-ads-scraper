#!/usr/bin/env python3
"""
Migrate companies from Railway SQLite → Supabase via edge function.
Run from local machine: python migrate_to_supabase.py
"""

import urllib.request
import json
import os
import time
import sys

RAILWAY_EXPORT_URL = "https://linkedin-scraper-production-b037.up.railway.app/export/companies"
SUPABASE_INGEST_URL = "https://raufchclngrralnzvags.supabase.co/functions/v1/ingest-companies"
INGEST_API_KEY = os.environ.get("SUPABASE_INGEST_KEY", "")

BATCH_SIZE = 200  # records per request to Supabase


def fetch_all_companies():
    """Fetch all companies from Railway."""
    print("Fetching companies from Railway...")
    req = urllib.request.Request(RAILWAY_EXPORT_URL)
    with urllib.request.urlopen(req, timeout=120) as resp:
        data = json.loads(resp.read().decode())
    companies = data.get("companies", data) if isinstance(data, dict) else data
    print(f"  Fetched {len(companies)} companies")
    return companies


def map_to_supabase(company: dict) -> dict:
    """Map Railway company fields to Supabase schema."""
    return {
        "advertiser_name": company["advertiser_name"],
        "ad_type": company.get("ad_type", "company_ad"),
        "company_id": company.get("company_id"),
        "company_url": company.get("company_url"),
        "profile_url": company.get("profile_url"),
        "promoted_by_name": company.get("promoted_by_name"),
        "promoted_by_company_id": company.get("promoted_by_company_id"),
        # first_seen_country in Railway = discovery_range in Supabase
        "discovery_range": company.get("first_seen_country"),
    }


def send_batch(batch: list, batch_num: int, total_batches: int) -> dict:
    """Send a batch to Supabase ingest endpoint."""
    body = json.dumps(batch).encode()
    req = urllib.request.Request(
        SUPABASE_INGEST_URL,
        data=body,
        headers={
            "x-api-key": INGEST_API_KEY,
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            result = json.loads(resp.read().decode())
            return {"success": True, "count": result.get("count", len(batch))}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()[:200]
        return {"success": False, "status": e.code, "error": error_body}
    except Exception as e:
        return {"success": False, "status": 0, "error": str(e)[:200]}


def migrate():
    companies = fetch_all_companies()

    # Map to Supabase format
    records = [map_to_supabase(c) for c in companies]
    print(f"Mapped {len(records)} records")

    # Send in batches
    total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    total_sent = 0
    total_failed = 0
    failed_batches = []

    print(f"\nSending {len(records)} records in {total_batches} batches of {BATCH_SIZE}...")

    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1

        result = send_batch(batch, batch_num, total_batches)

        if result["success"]:
            total_sent += result["count"]
            print(f"  Batch {batch_num}/{total_batches}: {result['count']} records ✓  (total: {total_sent})")
        else:
            total_failed += len(batch)
            failed_batches.append(batch_num)
            print(f"  Batch {batch_num}/{total_batches}: FAILED — {result.get('error', 'unknown')}")

        # Small delay to avoid rate limiting
        if batch_num < total_batches:
            time.sleep(0.5)

    print(f"\n{'='*50}")
    print(f"Migration complete!")
    print(f"  Sent: {total_sent}")
    print(f"  Failed: {total_failed}")
    if failed_batches:
        print(f"  Failed batches: {failed_batches}")

    return total_failed == 0


if __name__ == "__main__":
    success = migrate()
    sys.exit(0 if success else 1)
