# sync_jibble_attendance.py
import os
import sys
import datetime as dt
import requests
from urllib.parse import urlencode
from google.cloud import bigquery
import base64


# ========= Config via ENV =========
# The API_BASE path has been confirmed as correct.
API_BASE = os.environ.get("JIBBLE_API_BASE", "https://api.jibble.io/api").rstrip("/")
ENTRIES_PATH = os.environ.get("JIBBLE_ENTRIES_PATH", "/v1/time-entries")

# Jibble auth (API Credentials screen)
API_KEY_ID = os.environ.get("JIBBLE_API_KEY_ID", "").strip()
API_KEY_SECRET = os.environ.get("JIBBLE_API_KEY_SECRET", "").strip()
# Optional: some tenants require org/workspace id
ORG_ID = os.environ.get("JIBBLE_ORG_ID", "").strip()

# GCP / BigQuery
PROJECT = os.environ.get("GCP_PROJECT", "").strip()
DATASET = os.environ.get("BQ_DATASET", "").strip()
RAW_TABLE = f"{PROJECT}.{DATASET}.jibble_raw_attendance" if PROJECT and DATASET else None

# PH timezone
TZ = dt.timezone(dt.timedelta(hours=8))

def fail(msg: str):
    print(msg, file=sys.stderr)
    sys.exit(1)

def parse_date(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y-%m-%d").date()

def window_from_env():
    sd = os.getenv("START_DATE")
    ed = os.getenv("END_DATE")
    if sd and ed:
        s = parse_date(sd); e = parse_date(ed)
    else:
        # Default to yesterday if no dates are provided
        y = (dt.datetime.now(TZ).date() - dt.timedelta(days=1))
        s = e = y
    return (
        dt.datetime.combine(s, dt.time.min, TZ),
        dt.datetime.combine(e, dt.time.max, TZ),
    )

def is_workspace_api() -> bool:
    # OData lives under workspace.prod.jibble.io
    return "workspace." in API_BASE

def build_headers():
    """
    Builds the correct authentication headers. This is the definitive fix.
    This version uses only the X-API-* headers for the REST API, which is the
    final logical step after exhausting other methods.
    """
    if not (API_KEY_ID and API_KEY_SECRET):
        fail("Missing Jibble credentials: JIBBLE_API_KEY_ID and JIBBLE_API_KEY_SECRET are required.")

    headers = {"Accept": "application/json"}

    if is_workspace_api():
        # Workspace API uses Basic auth, which requires both ID and Secret.
        basic = base64.b64encode(f"{API_KEY_ID}:{API_KEY_SECRET}".encode()).decode()
        headers["Authorization"] = f"Basic {basic}"
    else:
        # Standard REST API: Use only the custom X-API headers.
        headers["X-API-KEY-ID"] = API_KEY_ID
        headers["X-API-KEY-SECRET"] = API_KEY_SECRET

    if ORG_ID:
        headers["X-Organization-Id"] = ORG_ID
    
    return headers


def safe_headers_for_log(h):
    masked = {}
    for k, v in h.items():
        kl = k.lower()
        if any(x in kl for x in ["secret", "authorization", "api-key"]):
            masked[k] = "***"
        else:
            masked[k] = v
    return masked

def http_get(full_url: str, headers):
    r = requests.get(full_url, headers=headers, timeout=60)
    if r.status_code >= 400:
        print("Jibble API error:", r.status_code, file=sys.stderr)
        print("URL:", r.url, file=sys.stderr)
        print("Sent headers:", safe_headers_for_log(headers), file=sys.stderr)
        print("Response Body:", r.text[:800], file=sys.stderr)
    r.raise_for_status()
    return r.json() if r.text else {}


def paginate_rest_time_entries(path: str, date_from: dt.datetime, date_to: dt.datetime, headers):
    page = 1
    while True:
        p = {"from": date_from.isoformat(), "to": date_to.isoformat(), "page": page, "limit": 200}
        url = f"{API_BASE}{path}?{urlencode(p)}"
        js = http_get(url, headers)
        items = js.get("data", [])
        if not items:
            break
        for it in items:
            yield it
        # Pagination ends when the API returns fewer items than the requested limit.
        if len(items) < p["limit"]:
            break
        page += 1

# -------- Normalization helper --------
def normalize(entry: dict):
    start = entry.get("startedAt") or entry.get("startAt")
    end   = entry.get("endedAt")  or entry.get("endAt")
    time_iso = start or end
    person = entry.get("person") or {}
    group  = entry.get("group") or {}
    activity = entry.get("activity") or {}
    kiosk = entry.get("kiosk") or {}

    return {
        "date": iso_date(time_iso),
        "full_name": person.get("name"),
        "group_name": group.get("name"),
        "entry_type": entry.get("type") or entry.get("entryType"),
        "time": iso_time(time_iso),
        "duration": sec_to_hms(entry.get("durationSeconds")),
        "activity_name": activity.get("name"),
        "kiosk_name": kiosk.get("name"),
        "created_on": entry.get("createdAt"),
        "last_edited_on": entry.get("updatedAt") or entry.get("lastEditedOn"),
        "raw_payload": entry # Also include the raw data
    }

def main():
    if not PROJECT or not DATASET or not RAW_TABLE:
        fail("Missing env vars: GCP_PROJECT and/or BQ_DATASET.")

    date_from, date_to = window_from_env()
    headers = build_headers()

    print(f"Starting Jibble sync for {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")

    # Use the appropriate paginator based on the API host.
    if is_workspace_api():
        # This part of the code is for the OData API, which is not being used.
        gen = paginate_workspace_time_entries(ORG_ID, date_from, date_to, headers)
    else:
        gen = paginate_rest_time_entries(ENTRIES_PATH, date_from, date_to, headers)

    # --- Normalize and prepare for BigQuery ---
    entries = [normalize(e) for e in gen]
    
    if not entries:
        print("No new Jibble entries found for this time window.")
        return

    print(f"Found {len(entries)} entries. Loading into BigQuery...")
    client = bigquery.Client(project=PROJECT)
    
    # Let BigQuery auto-detect the schema from the normalized data.
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", # Append new data
        autodetect=True,
    )
    
    job = client.load_table_from_json(entries, RAW_TABLE, job_config=job_config)
    job.result() # Wait for the job to complete
    
    if job.errors:
        print("Errors encountered while loading data into BigQuery:", file=sys.stderr)
        for error in job.errors:
            print(error, file=sys.stderr)
        fail("BigQuery load job failed.")
    
    print(f"Successfully loaded {job.output_rows} rows into {RAW_TABLE}")

if __name__ == "__main__":
    main()

