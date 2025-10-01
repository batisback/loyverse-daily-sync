# sync_jibble_attendance.py
import os
import sys
import datetime as dt
import requests
from urllib.parse import urlencode
from google.cloud import bigquery
import base64


# ========= Config via ENV =========
API_BASE = os.environ.get("JIBBLE_API_BASE", "https://api.jibble.io/api").rstrip("/")
# NOTE: ENTRIES_PATH is only used for REST host (api.jibble.io). On workspace host we ignore it.
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
    # OData lives under workspace.prod.jibble.io (and similar)
    return "workspace." in API_BASE

def build_headers():
    if not (API_KEY_ID and API_KEY_SECRET):
        fail("Missing Jibble credentials: set JIBBLE_API_KEY_ID and JIBBLE_API_KEY_SECRET.")

    # These are the primary headers for the standard REST API
    headers = {
        "Accept": "application/json",
        "X-API-KEY-ID": API_KEY_ID,
        "X-API-KEY-SECRET": API_KEY_SECRET,
    }

    # The OData (workspace) API uses Basic Auth instead, so we switch if needed
    if is_workspace_api():
        basic = base64.b64encode(f"{API_KEY_ID}:{API_KEY_SECRET}".encode()).decode()
        headers["Authorization"] = f"Basic {basic}"
        # Remove the X-API headers as they are not needed for OData
        headers.pop("X-API-KEY-ID", None)
        headers.pop("X-API-KEY-SECRET", None)

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

# -------- OData paginator (workspace host) --------
def paginate_workspace_time_entries(org_id: str, date_from: dt.datetime, date_to: dt.datetime, headers):
    if not org_id:
        fail("JIBBLE_ORG_ID is required when using workspace API.")
    top = 200
    skip = 0
    base = f"{API_BASE}/v1/Organizations/{org_id}/TimeEntries"
    filt = f"startedAt ge {date_from.isoformat()} and startedAt le {date_to.isoformat()}"

    while True:
        q = {"$filter": filt, "$orderby": "startedAt asc", "$top": top, "$skip": skip, "$format": "json"}
        url = f"{base}?{urlencode(q)}"
        js = http_get(url, headers)
        items = js.get("value", []) if isinstance(js, dict) else js
        if not items:
            break
        for it in items:
            yield it
        if len(items) < top:
            break
        skip += top


# -------- REST paginator (api host) --------
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
        # The REST API pagination is simpler: if we get fewer items than the limit, it's the last page.
        if len(items) < p["limit"]:
            break
        page += 1

# -------- Normalization helpers --------
def iso_date(ts_iso): return ts_iso[:10] if ts_iso else None

def iso_time(ts_iso):
    if not ts_iso: return None
    try: return ts_iso.split("T")[1][:8]
    except Exception: return None

def sec_to_hms(sec):
    if sec is None: return None
    sec = int(sec); h = sec//3600; m = (sec%3600)//60; s = sec%60
    return f"{h:02d}:{m:02d}:{s:02d}"

def normalize(entry: dict):
    # Works for both OData and REST if fields are named similarly
    start = entry.get("startedAt") or entry.get("startAt")
    end   = entry.get("endedAt")  or entry.get("endAt")
    time_iso = start or end
    person = entry.get("person") or {}
    group  = entry.get("group") or {}
    activity = entry.get("activity") or {}
    kiosk = entry.get("kiosk") or {}

    return {
        "Date": iso_date(time_iso),
        "Full_Name": person.get("name"),
        "Group": group.get("name") or None,
        "Entry_Type": entry.get("type") or entry.get("entryType"),
        "Time": iso_time(time_iso),
        "Duration": sec_to_hms(entry.get("durationSeconds")) if entry.get("durationSeconds") is not None else None,
        "Activity": activity.get("name"),
        "Kiosk_Name": kiosk.get("name"),
        "Created_On": entry.get("createdAt"),
        "Last_Edited_On": entry.get("updatedAt") or entry.get("lastEditedOn"),
    }

def main():
    if not PROJECT or not DATASET or not RAW_TABLE:
        fail("Missing env vars: GCP_PROJECT and/or BQ_DATASET.")

    date_from, date_to = window_from_env()
    headers = build_headers()

    print(f"Starting Jibble sync for {date_from.strftime('%Y-%m-%d')} to {date_to.strftime('%Y-%m-%d')}")

    # --- Choose which paginator to use based on the API base URL ---
    if is_workspace_api():
        print("Using OData API (workspace) paginator.")
        gen = paginate_workspace_time_entries(ORG_ID, date_from, date_to, headers)
    else:
        print("Using REST API (api.jibble.io) paginator.")
        gen = paginate_rest_time_entries(ENTRIES_PATH, date_from, date_to, headers)

    # --- Normalize + load into BQ ---
    # The BigQuery client requires a list of dicts.
    entries = [{"json": e} for e in gen]
    
    if not entries:
        print("No new Jibble entries found for this time window.")
        return

    print(f"Found {len(entries)} entries. Loading into BigQuery...")
    client = bigquery.Client(project=PROJECT)

    # Define the schema for the raw table to handle JSON data
    schema = [
        bigquery.SchemaField("json", "JSON"),
    ]
    job_config = bigquery.LoadJobConfig(schema=schema)
    
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
