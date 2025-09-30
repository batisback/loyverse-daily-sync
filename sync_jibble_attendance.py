# sync_jibble_attendance.py
import os
import sys
import json
import datetime as dt
import requests
from google.cloud import bigquery

# ========= Config via ENV =========
API_BASE = os.environ.get("JIBBLE_API_BASE", "https://api.jibble.io/api").rstrip("/")
ENTRIES_PATH = os.environ.get("JIBBLE_ENTRIES_PATH", "/v1/time-entries")  # try /v2/time-entries if needed

# Preferred auth: client credentials (ID/SECRET) -> OAuth token
CLIENT_ID = os.environ.get("JIBBLE_CLIENT_ID", "").strip()
CLIENT_SECRET = os.environ.get("JIBBLE_CLIENT_SECRET", "").strip()

# Fallback: static token (rare). If set, we'll use it.
STATIC_TOKEN = os.environ.get("JIBBLE_API_TOKEN", "").strip()

# Optional: some tenants need an org/workspace id
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
        s = parse_date(sd)
        e = parse_date(ed)
    else:
        y = (dt.datetime.now(TZ).date() - dt.timedelta(days=1))
        s = e = y
    return (
        dt.datetime.combine(s, dt.time.min, TZ),
        dt.datetime.combine(e, dt.time.max, TZ),
    )


# ---------- OAuth token (client credentials) ----------
def get_access_token() -> str:
    if CLIENT_ID and CLIENT_SECRET:
        base_no_api = API_BASE.replace("/api", "")  # e.g., https://api.jibble.io
        candidates = [
            f"{API_BASE}/oauth/token",       # .../api/oauth/token
            f"{API_BASE}/oauth2/token",      # .../api/oauth2/token
            f"{base_no_api}/oauth/token",    # .../oauth/token
            f"{base_no_api}/oauth2/token",   # .../oauth2/token
        ]

        data = {
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }

        chosen = None
        for url in candidates:
            try:
                r = requests.post(url, data=data, timeout=60)
                if r.status_code >= 400:
                    print("OAuth error:", r.status_code, "URL:", url, "Body:", r.text[:800])
                    continue
                js = r.json()
                tok = js.get("access_token")
                if tok:
                    print("OAuth success via:", url)
                    return tok
                else:
                    print("OAuth response missing access_token from", url, "body:", js)
            except Exception as e:
                print("OAuth exception for", url, "->", e)

        fail("Could not obtain access_token from any known token endpoint.")

    if STATIC_TOKEN:
        return STATIC_TOKEN

    fail("Missing credentials: set JIBBLE_CLIENT_ID / JIBBLE_CLIENT_SECRET (or JIBBLE_API_TOKEN).")



def build_headers():
    tok = get_access_token()
    h = {"Accept": "application/json", "Authorization": f"Bearer {tok}"}
    if ORG_ID:
        h["X-Organization-Id"] = ORG_ID
        h["X-Org-Id"] = ORG_ID
    return h


def fetch(path, params, headers):
    url = f"{API_BASE}{path}"
    r = requests.get(url, headers=headers, params=params, timeout=60)
    if r.status_code >= 400:
        safe = {k: ("***" if k.lower() in ("authorization", "x-api-key") else v) for k, v in headers.items()}
        print("Jibble API error:", r.status_code)
        print("URL:", r.url)
        print("Sent headers:", safe)
        print("Body:", r.text[:800])
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        fail("Failed to parse JSON response from Jibble")


def paginate(path, params, headers, data_key="data"):
    page = 1
    while True:
        p = dict(params)
        p.update({"page": page, "limit": 200})
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
    if not PROJECT or not DATASET:
        fail("Missing env vars: GCP_PROJECT and/or BQ_DATASET")
    if not RAW_TABLE:
        fail("Could not construct RAW_TABLE; check GCP_PROJECT/BQ_DATASET")

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
