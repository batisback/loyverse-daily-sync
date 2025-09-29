import os, json, datetime as dt, requests, sys
from google.cloud import bigquery

entries = [ {"payload": normalize(e)} for e in paginate(ENTRIES_PATH, params) ]

API_BASE = os.environ.get("JIBBLE_API_BASE", "https://api.jibble.io")
TOKEN = os.environ["JIBBLE_API_TOKEN"]
HEADERS = {"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}

PROJECT = os.environ["GCP_PROJECT"]
DATASET = os.environ["BQ_DATASET"]  # set to sbco_ops_jibble in workflow
RAW_TABLE = f"{PROJECT}.{DATASET}.jibble_raw_attendance"
TZ = dt.timezone(dt.timedelta(hours=8))  # Asia/Manila

def parse_date(s):
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

def fetch(path, params):
    r = requests.get(f"{API_BASE}{path}", headers=HEADERS, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def paginate(path, params, data_key="data"):
    page = 1
    while True:
        p = dict(params); p.update({"page": page, "limit": 200})
        js = fetch(path, p)
        items = js.get(data_key, js if isinstance(js, list) else [])
        if not items: break
        for it in items: yield it
        if len(items) < p["limit"] or not js.get("nextPage"): break
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

def normalize(entry):
    # Adjust these keys if your Jibble API uses different names
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
    date_from, date_to = window_from_env()
    params = {"from": date_from.isoformat(), "to": date_to.isoformat()}
    # Endpoint path may differ in your account; adjust if needed
    entries = [ {"payload": normalize(e)} for e in paginate(ENTRIES_PATH, params) ]

    if not entries:
        print("No rows for window."); return
    client = bigquery.Client(project=PROJECT)
    job = client.load_table_from_json(entries, RAW_TABLE)
    job.result()
    print(f"Loaded {len(entries)} rows into {RAW_TABLE}")

if __name__ == "__main__":
    if not TOKEN or not PROJECT or not DATASET:
        print("Missing env vars JIBBLE_API_TOKEN / GCP_PROJECT / BQ_DATASET", file=sys.stderr)
        sys.exit(1)
    main()
