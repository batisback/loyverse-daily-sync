import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# 🔐 Loyverse API setup (RECOMMENDED: use env var / GitHub secret)
LOYVERSE_TOKEN = os.getenv("LOYVERSE_TOKEN", "PUT_TOKEN_HERE_TEMP")  # <- ideally remove hardcode
HEADERS = {
    "Authorization": f"Bearer {LOYVERSE_TOKEN}",
    "Content-Type": "application/json"
}
RECEIPT_URL = "https://api.loyverse.com/v1.0/receipts"
SHIFT_URL = "https://api.loyverse.com/v1.0/shifts"

# 📊 BigQuery setup
project_id = "loyverse-anomaly-warehouse"
dataset_id = "loyverse_data"
client = bigquery.Client(project=project_id)

def fix_shifts_df(df):
    if "taxes" in df.columns:
        df["taxes"] = df["taxes"].apply(lambda x: [int(t) for t in x] if isinstance(x, list) else [])
    if "cash_movements" in df.columns:
        def fix_movements(x):
            if isinstance(x, list) and all(isinstance(i, dict) for i in x):
                return x
            return []
        df["cash_movements"] = df["cash_movements"].apply(fix_movements)
    return df

def pull_and_upload(entity_name, url, key_name, utc_start, utc_end, date_str):
    print(f"\n📦 Pulling {entity_name} for {date_str}...")

    data_collected = []
    params = {
        "created_at_min": utc_start,
        "created_at_max": utc_end,
        "limit": 250
    }

    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"❌ Error {response.status_code}: {response.text}")
            return False

        json_data = response.json()
        data_collected.extend(json_data.get(key_name, []))

        next_cursor = json_data.get("cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor

    if not data_collected:
        print(f"⚠️ No {entity_name} found for {date_str}.")
        return True

    df = pd.json_normalize(data_collected, sep="_")
    if entity_name == "shifts":
        df = fix_shifts_df(df)

    table_id = f"{project_id}.{dataset_id}.{entity_name}_{date_str}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()

    print(f"✅ Uploaded {len(df)} records to BigQuery: {table_id}")
    return True

def merge_into_final(table_type, date_str):
    temp_table = f"{dataset_id}.{table_type}_{date_str}"
    final_table = f"{dataset_id}.new_final_{table_type}"  # <-- change to final_{table_type} if that’s your real name
    id_field = "receipt_number" if table_type == "receipts" else "id"

    merge_sql = f"""
        MERGE `{project_id}.{final_table}` AS target
        USING `{project_id}.{temp_table}` AS source
        ON target.{id_field} = source.{id_field}
        WHEN NOT MATCHED THEN
          INSERT ROW
    """

    print(f"🔁 Merging {table_type}_{date_str} into {final_table}...")
    client.query(merge_sql).result()
    print(f"✅ Merge complete for {final_table}")

# ==================== 6-DAY BACKFILL ====================
today_ph = datetime.now()
DAYS_TO_BACKFILL = 6

for i in range(DAYS_TO_BACKFILL, 0, -1):
    day_start = (today_ph - timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
    start_ph = day_start
    end_ph = day_start.replace(hour=23, minute=59, second=59, microsecond=999999)

    utc_start = (start_ph - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    utc_end   = (end_ph   - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    date_str  = day_start.strftime("%Y_%m_%d")

    print(f"\n==================== PROCESSING DATE: {date_str} ====================")

    ok = pull_and_upload("receipts", RECEIPT_URL, "receipts", utc_start, utc_end, date_str)
    if ok:
        merge_into_final("receipts", date_str)

    ok = pull_and_upload("shifts", SHIFT_URL, "shifts", utc_start, utc_end, date_str)
    if ok:
        merge_into_final("shifts", date_str)

print("\n✅ 6-DAY BACKFILL COMPLETE ✅")
