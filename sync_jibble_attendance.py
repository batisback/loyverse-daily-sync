# sync_jibble_attendance.py
import os
import sys
import json
import datetime as dt
import requests
from google.cloud import bigquery

# ========= Config via ENV (with safe defaults) =========
# You can change these from the GitHub Actions workflow env without editing this file.

API_BASE = os.environ.get("JIBBLE_API_BASE", "https://api.jibble.io/api").rstrip("/")
ENTRIES_PATH = os.environ.get("JIBBLE_ENTRIES_PATH", "/v1/time-entries")  # try /v2/time-entries or /v1/time-tracking/time-entries if needed

# Auth style: "api-key" (X-API-KEY: <key>) OR "bearer" (Authorization: Bearer <key>)
AUTH_STYLE = os.environ.get("JIBBLE_AUTH_STYLE", "api-key").strip().lower()
JIBBLE_API_TOKEN = os.environ.get("JIBBLE_API_TOKEN", "").strip()
JIBBLE_ORG_ID = os.environ.get("JIBBLE_ORG_ID", "").strip()  # optional

# GCP / BigQuery
PROJECT = os.environ.get("GCP_PROJECT", "").strip()
DATASET = os.environ.get("BQ_DATASET", "").strip()  # e.g., sbco_ops_jibble
RAW_TABLE = f"{PROJECT}.{DATASET}.jibble_raw_attendance" if PROJECT and DATASET else None

# Timezone: Asia/Manila
TZ = dt.timezone(dt.timedelta(hours=8))

# ========= Helpers =========

def build_headers():
    """Build API headers based on AUTH_STYLE and optional org id."""
    if not JIBBLE_API_TOKEN:
        _fail("Missing env var JIBBLE_API_TOKEN")

    headers = {"Accept": "application/json"}
    if AUTH_STYLE == "api-key":
        headers["X-API-KEY"] = JIBBLE_API_TOKEN
    else:
        headers["Authorization"] = f"Bearer {JIBBLE_API_TOKEN}"

    # If your org requires it, include org id headers (harmless if unused)
    if JIBBLE_ORG_ID:
        headers["X-Organization-Id"] = JIBBLE_ORG_ID
        headers["X-Org-Id"] = JIBBLE_ORG_ID

    return headers


def _fail(msg):
    print(msg, file=sys.stderr)
    sys.exit(1)


def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()


def window_from_env():
    """Use START_DATE/END_DATE (YYYY-MM-DD) if provided, else yesterday (PH)."""
    sd = os.getenv("START_DATE")
    ed = os.getenv("END_DATE")
    if sd and ed:
        s = parse_date(sd)
        e = parse_date(ed)
    else:
        y = (dt.datetime.now(TZ).date() - dt.timedelta(days=1))
        s = e = y
    return (
        dt.datetime.combine(s, dt.time.min, TZ),
        dt.datetime.combine(e, dt.time.max, TZ),
    )


def fetch(path, params, headers):
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=60)

    if r.status_code >= 400:
        # Mask sensitive headers in logs
        safe_headers = {}
        for k, v in headers.items():
            if k.lower() in ("authorization", "x-api-key"):
                safe_headers[k] = "***"
            else:
                safe_headers[k] = v
        print("Jibble API error:", r.status_code)
        print("URL:", r.url)
        print("Sent headers:", safe_headers)
        print("Body:", r.text[:800])

    r.raise_for_status()
    # Jibble may return a list or a dict
    try:
        return r.json()
    except Exception:
        _fail("Failed to parse JSON response from Jibble")


def paginate(path, params, headers, data_key="data"):
    """Yield items across pages. Works if API returns list or {data: [], nextPage: ...}."""
    page = 1
    while True:
        p = dict(params)
        p.update({"page": page, "limit": 200})
        js = fetch(path, p, headers)

        # Normalize items
        if isinstance(js, list):
