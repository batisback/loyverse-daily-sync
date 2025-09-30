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
            items = js
            next_page_exists = len(items) == p["limit"]
        else:
            items = js.get(data_key, [])
            next_page_exists = bool(js.get("nextPage")) or len(items) == p["limit"]

        if not items:
            break

        for it in items:
            yield it

        if not next_page_exists:
            break

        page += 1


def iso_date(ts_iso):
    return ts_iso[:10] if ts_iso else None


def iso_time(ts_iso):
    if not ts_iso:
        return None
    try:
        return ts_iso.split("T")[1][:8]
    except Exception:
        return None


def sec_to_hms(sec):
    if sec is None:
        return None
    sec = int(sec)
    h = sec // 3600
    m = (sec % 3600) // 60
    s = sec % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def normalize(entry: dict):
    """
    Map API fields to the 8 columns we want (plus created/updated for MERGE freshness).
    If your account uses slightly different field names, adjust here.
    """
    start = entry.get("startedAt") or entry.get("startAt")
    end = entry.get("endedAt") or entry.get("endAt")
    time_iso = start or end

    return {
        "Date": iso_date(time_iso),
        "Full Name": (entry.get("person") or {}).get("name"),
        "Group": ((entry.get("group") or {}).get("name")) or None,
        "EntryType": entry.get("type") or entry.get("entryType"),
        "Time": iso_time(time_iso),
        "Duration": sec_to_hms(entry.get("durationSeconds")) if entry.get("durationSeconds") is not None else None,
        "Activity": (entry.get("activity") or {}).get("name"),
        "Kiosk Name": (entry.get("kiosk") or {}).get("name"),
        "Created On": entry.get("createdAt"),
        "Last Edited On": entry.get("updatedAt") or entry.get("lastEditedOn"),
    }


def main():
    # Validate required envs
    if not PROJECT or not DATASET:
        _fail("Missing env vars: GCP_PROJECT and/or BQ_DATASET")

    if not RAW_TABLE:
        _fail("Could not construct RAW_TABLE; check GCP_PROJECT/BQ_DATASET")

    # Date window in PH time
    date_from, date_to = window_from_env()
    params = {"from": date_from.isoformat(), "to": date_to.isoformat()}

    headers = build_headers()

    # Pull entries
    entries = [{"payload": normalize(e)} for e in paginate(ENTRIES_PATH, params, headers)]
    if not entries:
        print("No rows for window.")
        return

    # Load into BigQuery RAW
    client = bigquery.Client(project=PROJECT)
    job = client.load_table_from_json(entries, RAW_TABLE)
    job.result()
    print(f"Loaded {len(entries)} rows into {RAW_TABLE}")


if __name__ == "__main__":
    main()
