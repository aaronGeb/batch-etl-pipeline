"""
fetch_api_data.py

Pulls a daily market-data snapshot from the CoinGecko public API
(/coins/markets) and lands the raw JSON in S3, partitioned by run date.

S3 layout:
    s3://<bucket>/raw/coingecko/market_snapshot/dt=<run_date>/market_snapshot.json

Designed to be called two ways:
  1. As a CLI script (for local testing):
       python fetch_api_data.py --run-date 2026-08-25
  2. As an importable module from an Airflow PythonOperator:
       from scripts.fetch_api_data import run
       run(run_date="2026-08-25")

Config comes from environment variables (see .env.example):
    SOURCE_API_BASE_URL   e.g. https://api.coingecko.com/api/v3
    AWS_ACCESS_KEY_ID
    AWS_SECRET_ACCESS_KEY
    AWS_DEFAULT_REGION
    S3_BUCKET_NAME
    COINGECKO_VS_CURRENCY   default: usd
    COINGECKO_PER_PAGE      default: 100 (CoinGecko max per page is 250)
    COINGECKO_PAGES         default: 1
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any

import boto3
import requests
from botocore.exceptions import BotoCoreError, ClientError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("fetch_api_data")

DEFAULT_BASE_URL = "https://api.coingecko.com/api/v3"
DEFAULT_VS_CURRENCY = "usd"
DEFAULT_PER_PAGE = 100
DEFAULT_PAGES = 1
REQUEST_TIMEOUT_SECONDS = 30
SECONDS_BETWEEN_PAGE_REQUESTS = 2.0  # be polite to CoinGecko's free-tier rate limit


def _build_session() -> requests.Session:
    """HTTP session with retry/backoff for transient failures and rate limits."""
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,  # 2s, 4s, 8s, 16s, 32s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def fetch_coin_markets_page(
    session: requests.Session,
    base_url: str,
    vs_currency: str,
    per_page: int,
    page: int,
) -> list[dict[str, Any]]:
    """Fetch a single page of the /coins/markets endpoint."""
    url = f"{base_url.rstrip('/')}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }

    logger.info("Fetching CoinGecko markets page %s (per_page=%s)", page, per_page)
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    data = response.json()
    if not isinstance(data, list):
        raise ValueError(
            f"Unexpected response shape from CoinGecko (expected list): {type(data)}"
        )
    logger.info("Page %s returned %s coins", page, len(data))
    return data


def fetch_all_pages(
    base_url: str,
    vs_currency: str,
    per_page: int,
    pages: int,
) -> list[dict[str, Any]]:
    """Fetch and concatenate `pages` pages of market data."""
    session = _build_session()
    all_records: list[dict[str, Any]] = []

    for page_number in range(1, pages + 1):
        page_data = fetch_coin_markets_page(
            session=session,
            base_url=base_url,
            vs_currency=vs_currency,
            per_page=per_page,
            page=page_number,
        )
        all_records.extend(page_data)

        if page_number < pages:
            time.sleep(SECONDS_BETWEEN_PAGE_REQUESTS)

    return all_records


def upload_to_s3(records: list[dict[str, Any]], bucket: str, key: str) -> None:
    """Upload the fetched records to S3 as a single JSON document."""
    s3_client = boto3.client("s3")
    body = json.dumps(records, ensure_ascii=False).encode("utf-8")

    logger.info("Uploading %s records to s3://%s/%s", len(records), bucket, key)
    try:
        s3_client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/json"
        )
    except (BotoCoreError, ClientError):
        logger.exception("Failed to upload market snapshot to S3")
        raise

    logger.info("Upload complete: s3://%s/%s", bucket, key)


def run(
    run_date: str | None = None,
    base_url: str | None = None,
    vs_currency: str | None = None,
    per_page: int | None = None,
    pages: int | None = None,
    bucket: str | None = None,
) -> str:
    """
    Fetch a CoinGecko market snapshot and land it in S3.

    Returns the S3 key the data was written to, so callers (e.g. an
    Airflow task) can pass it downstream via XCom.
    """
    run_date = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    base_url = base_url or os.environ.get("SOURCE_API_BASE_URL", DEFAULT_BASE_URL)
    vs_currency = vs_currency or os.environ.get(
        "COINGECKO_VS_CURRENCY", DEFAULT_VS_CURRENCY
    )
    per_page = per_page or int(os.environ.get("COINGECKO_PER_PAGE", DEFAULT_PER_PAGE))
    pages = pages or int(os.environ.get("COINGECKO_PAGES", DEFAULT_PAGES))
    bucket = bucket or os.environ.get("S3_BUCKET_NAME")

    if not bucket:
        raise EnvironmentError("S3_BUCKET_NAME is not set")

    records = fetch_all_pages(
        base_url=base_url,
        vs_currency=vs_currency,
        per_page=per_page,
        pages=pages,
    )

    # Stamp each record with the run date and extraction timestamp so
    # downstream dbt models can trust these fields instead of relying
    # on S3 object metadata.
    extracted_at = datetime.now(timezone.utc).isoformat()
    for record in records:
        record["_run_date"] = run_date
        record["_extracted_at"] = extracted_at

    s3_key = f"raw/coingecko/market_snapshot/dt={run_date}/market_snapshot.json"
    upload_to_s3(records=records, bucket=bucket, key=s3_key)

    return s3_key


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-date",
        default=None,
        help="Partition date (YYYY-MM-DD). Defaults to today (UTC).",
    )
    parser.add_argument("--base-url", default=None, help="Override SOURCE_API_BASE_URL")
    parser.add_argument("--vs-currency", default=None, help="e.g. usd, eur")
    parser.add_argument(
        "--per-page", type=int, default=None, help="Coins per page (max 250)"
    )
    parser.add_argument(
        "--pages", type=int, default=None, help="Number of pages to fetch"
    )
    parser.add_argument("--bucket", default=None, help="Override S3_BUCKET_NAME")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    s3_key = run(
        run_date=args.run_date,
        base_url=args.base_url,
        vs_currency=args.vs_currency,
        per_page=args.per_page,
        pages=args.pages,
        bucket=args.bucket,
    )
    print(s3_key)


if __name__ == "__main__":
    main()
