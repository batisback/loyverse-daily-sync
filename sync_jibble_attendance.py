# sync_jibble_attendance.py
import os
import sys
import datetime as dt
import requests
from google.cloud import bigquery

# ========= Config via ENV =========
API_BASE = os.environ.get("JIBBLE_API_BASE", "https://api.jibble.io/api").rstrip("/")
ENTRIES_PATH = os.environ.get("JIBBLE_ENTRIES_PATH", "/v1/time-entries")  # try /v2/time-entries if needed

# Auth (preferred): API Key ID + API Key Secret (from Jibble "API Credentials" screen)
API_KEY_ID = os.environ.get("JIBBLE_API_KEY_ID", "").strip()
API_KEY_SECRET = os.environ.get("JIBBLE_API_KEY_SECRET", "").strip()

# Optional fallback: personal access token (PAT) or legacy token
API_TOKEN = os.environ.get("JIBBLE_API_TOKEN", "").strip()
AUTH_STYLE = os.environ.get("JIBBLE_AUTH_STYLE", "api-key").lower()  # 'api-key' or 'bearer' for PATs

# Optional: some tenants need org/workspace id
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
        y = (dt.datetime.now(TZ).date() - dt.timedelta(days=1))
        s = e = y
    return (
        dt.datetime.combine(s, dt.time.min, TZ),
        dt.datetime.combine(e, dt.time.max, TZ),
    )

# ---------- Headers ----------
def build_headers():
    headers = {"Accept": "application/json"}

    if API_KEY_ID and API_KEY_SECRET:
        # what you already had
        headers["X-API-KEY-ID"] = API_KEY_ID
        headers["X-API-KEY-SECRET"] = API_KEY_SECRET

        # add both of these — some tenants require one of them
        headers["X-API-KEY"] = API_KEY_SECRET
        headers["Authorization"] = f"ApiKey {API_KEY_SECRET}"

    elif API_TOKEN:  # fallback if you ever switch to PAT
        if AUTH_STYLE == "bearer":
            headers["Authorization"] = f"Bearer {API_TOKEN}"
        else:
            headers["X-API-KEY"] = API_TOKEN
    else:
        fail(
            "Missing Jibble credentials: set JIBBLE_API_KEY_ID and JIBBLE_API_KEY_SECRET "
            "(or JIBBLE_API_TOKEN with JIBBLE_AUTH_STYLE=api-key|bearer)."
        )

    if ORG_ID:
        headers["X-Organization-Id"] = ORG_ID
        headers["X-Org-Id"] = ORG_ID

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

def fetch(path, params, headers):
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code >= 400:
        print("Jibble API error:", r.status_code)
        print("URL:", r.url)
        print("Sent headers:", safe_headers_for_log(headers))
        print("Body:", r.text[:800])
    r.raise_for_status()
    return r.json() if r.text else {}

def paginate(path, params, headers, data_key="data"):
    page = 1
    while True:
        p = dict(params); p.update({"page": page, "limit": 200})
        js = fetch(path, p, headers)

        if isinstance(js, list):
            items = js
            next_page = len(items) == p["limit"]
        else:
            items = js.get(data_key, [])
            next_page = bool(js.get("nextPage")) or len(items) == p["limit"]

        if not items:
            break

        for it in items:
            yield it

        if not next_page:
            break
        page += 1

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
    start = entry.get("startedAt") or entry.get("startAt")
    end   = entry.get("endedAt")  or entry.get("endAt")
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
    if not PROJECT or not DATASET or not RAW_TABLE:
        fail("Missing env vars: GCP_PROJECT and/or BQ_DATASET (needed for RAW_TABLE).")

    date_from, date_to = window_from_env()
    params = {"from": date_from.isoformat(), "to": date_to.isoformat()}
    headers = build_headers()

    entries = [{"payload": normalize(e)} for e in paginate(ENTRIES_PATH, params, headers)]
    if not entries:
        print("No rows for window.")
        return

    client = bigquery.Client(project=PROJECT)
    job = client.load_table_from_json(entries, RAW_TABLE)
    job.result()
    print(f"Loaded {len(entries)} rows into {RAW_TABLE}")

if __name__ == "__main__":
    main()
