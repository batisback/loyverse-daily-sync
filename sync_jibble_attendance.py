# sync_jibble_attendance.py  (API Key ID/Secret + workspace-aware)
import os, sys, json, datetime as dt, requests
from urllib.parse import urlencode, quote_plus
from google.cloud import bigquery

# --------- Config via ENV ---------
API_BASE = os.environ.get("JIBBLE_API_BASE", "https://api.jibble.io").rstrip("/")
ENTRIES_PATH = os.environ.get("JIBBLE_ENTRIES_PATH", "/v1/time-entries")  # used on classic REST host

# Jibble auth (API Credentials)
API_KEY_ID     = os.environ.get("JIBBLE_API_KEY_ID", "").strip()
API_KEY_SECRET = os.environ.get("JIBBLE_API_KEY_SECRET", "").strip()
ORG_ID         = os.environ.get("JIBBLE_ORG_ID", "").strip()  # required for workspace host

# GCP / BigQuery
PROJECT  = os.environ.get("GCP_PROJECT", "").strip()
DATASET  = os.environ.get("BQ_DATASET", "").strip()
RAW_TABLE = f"{PROJECT}.{DATASET}.jibble_raw_attendance" if PROJECT and DATASET else None

# PH timezone
TZ = dt.timezone(dt.timedelta(hours=8))


# ---------- helpers ----------
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
        y = (dt.datetime.now(TZ).date() - dt.timedelta(days=1))
        s = e = y
    start = dt.datetime.combine(s, dt.time.min, TZ)
    end   = dt.datetime.combine(e, dt.time.max, TZ)
    return start, end

def is_workspace_api() -> bool:
    # e.g. https://workspace.prod.jibble.io
    return "workspace." in API_BASE


# ---------- HTTP ----------
def build_headers():
    if not API_KEY_ID or not API_KEY_SECRET:
        fail("Missing JIBBLE_API_KEY_ID / JIBBLE_API_KEY_SECRET")
    h = {
        "Accept": "application/json",
        # workspace OData often expects these custom headers:
        "X-API-KEY-ID": API_KEY_ID,
        "X-API-KEY-SECRET": API_KEY_SECRET,
    }
    if ORG_ID:
        h["X-Organization-Id"] = ORG_ID
        h["X-Org-Id"] = ORG_ID  # harmless duplicate—some tenants use the shorter key
    return h

def http_get(url: str, headers: dict):
    r = requests.get(url, headers=headers, timeout=60)
    if r.status_code >= 400:
        safe = {k: ("***" if "key" in k.lower() or "auth" in k.lower() else v) for k,v in headers.items()}
        print("Jibble API error:", r.status_code)
        print("URL:", url)
        print("Sent headers:", safe)
        print("Body:", r.text[:800])
    r.raise_for_status()
    return r.json() if r.text else {}


# ---------- Pagination over different route families ----------
def paginate_rest_time_entries(path: str, start: dt.datetime, end: dt.datetime, headers: dict):
    """Classic REST: GET {API_BASE}{path}?from=...&to=...&page=1&limit=200"""
    page = 1
    while True:
        params = {
            "from": start.isoformat(),
            "to":   end.isoformat(),
            "page": page,
            "limit": 200,
        }
        url = f"{API_BASE}{path}?{urlencode(params)}"
        js = http_get(url, headers)
        items = js.get("data", js if isinstance(js, list) else [])
        if not items:
            break
        for it in items:
            yield it
        if len(items) < 200 or not js.get("nextPage"):
            break
        page += 1

def paginate_workspace_time_entries(org_id: str, start: dt.datetime, end: dt.datetime, headers: dict):
    """
    Workspace OData style:
      GET {API_BASE}/v1/Organizations/{org_id}/TimeEntries?$filter=startedAt ge ... and startedAt le ...&$orderby=startedAt asc&$top=200&$skip=N
    """
    # Workspace uses ISO with timezone. Use PH window values directly.
    frm = start.isoformat()
    to  = end.isoformat()

    top = 200
    skip = 0
    # Two common field names across tenants: startedAt or startAt. Try startedAt first.
    fields_to_try = ["startedAt", "startAt"]

    for field in fields_to_try:
        skip = 0
        while True:
            qs = [
                ("$filter", f"{field} ge {frm} and {field} le {to}"),
                ("$orderby", f"{field} asc"),
                ("$top", str(top)),
                ("$skip", str(skip)),
                ("$format", "json"),
            ]
            url = f"{API_BASE}/v1/Organizations/{org_id}/TimeEntries?{urlencode(qs, quote_via=quote_plus)}"
            js = http_get(url, headers)

            items = []
            if isinstance(js, dict):
                # OData commonly returns { "value": [ ... ] }
                items = js.get("value") or js.get("data") or []
            elif isinstance(js, list):
                items = js
            if not items:
                break

            for it in items:
                yield it

            if len(items) < top:
                break
            skip += top


# ---------- field normalization ----------
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
    # Try both naming conventions
    start = entry.get("startedAt") or entry.get("startAt")
    end   = entry.get("endedAt")  or entry.get("endAt")
    time_iso = start or end
    return {
        "Date": iso_date(time_iso),
        "Full Name": (entry.get("person") or {}).get("name") or entry.get("memberName"),
        "Group": ((entry.get("group") or {}).get("name")) or entry.get("groupName"),
        "EntryType": entry.get("type") or entry.get("entryType"),
        "Time": iso_time(time_iso),
        "Duration": sec_to_hms(entry.get("durationSeconds")) if entry.get("durationSeconds") is not None else None,
        "Activity": (entry.get("activity") or {}).get("name") or entry.get("activityName"),
        "Kiosk Name": (entry.get("kiosk") or {}).get("name") or entry.get("kioskName"),
        "Created On": entry.get("createdAt"),
        "Last Edited On": entry.get("updatedAt") or entry.get("lastEditedOn"),
    }


# ---------- main ----------
def main():
    if not PROJECT or not DATASET or not RAW_TABLE:
        fail("Missing env vars: GCP_PROJECT and/or BQ_DATASET (needed for RAW_TABLE).")

    start_dt, end_dt = window_from_env()

    headers = build_headers()

    # Optional: quick probe so logs show which route family we hit
    try:
        if is_workspace_api():
            if not ORG_ID:
                fail("JIBBLE_ORG_ID is required when using workspace host.")
            probe_url = f"{API_BASE}/v1/Organizations/{ORG_ID}"
        else:
            probe_url = f"{API_BASE}/v1/organizations"
        _ = http_get(probe_url, headers)
        print("Auth probe OK:", probe_url)
    except Exception as e:
        print("Auth probe failed (continuing):", e)

    # Pick the generator based on host
    if is_workspace_api():
        gen = paginate_workspace_time_entries(ORG_ID, start_dt, end_dt, headers)
    else:
        gen = paginate_rest_time_entries(ENTRIES_PATH, start_dt, end_dt, headers)

    entries = [{"payload": normalize(e)} for e in gen]
    if not entries:
        print("No rows for window.")
        return

    client = bigquery.Client(project=PROJECT)
    job = client.load_table_from_json(entries, RAW_TABLE)
    job.result()
    print(f"Loaded {len(entries)} rows into {RAW_TABLE}")


if __name__ == "__main__":
    main()
