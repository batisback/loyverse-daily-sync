import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# 🔐 Loyverse API
LOYVERSE_TOKEN = "53dbaaeae21541fb89080b0688fc0969"
HEADERS = {
    "Authorization": f"Bearer {LOYVERSE_TOKEN}",
    "Content-Type": "application/json"
}
RECEIPT_URL = "https://api.loyverse.com/v1.0/receipts"
SHIFT_URL = "https://api.loyverse.com/v1.0/shifts"

# 📊 BigQuery
project_id = "loyverse-anomaly-warehouse"
dataset_id = "loyverse_data"
client = bigquery.Client(project=project_id)

# 📆 Set date range for the previous day in PH time (UTC+8)
today_ph = datetime.now()
yesterday_ph = today_ph - timedelta(days=1)

ph_start = yesterday_ph.replace(hour=0, minute=0, second=0, microsecond=0)
ph_end = yesterday_ph.replace(hour=23, minute=59, second=59, microsecond=999999)

# Convert to UTC
utc_start = (ph_start - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
utc_end = (ph_end - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
date_str = ph_start.strftime("%Y_%m_%d")

def pull_and_upload(entity_name, url, key_name):
    print(f"📦 Pulling {entity_name} for {ph_start.strftime('%Y-%m-%d')}...")
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
            return

        json_data = response.json()
        data_collected.extend(json_data.get(key_name, []))

        next_cursor = json_data.get("cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor

    if data_collected:
        df = pd.json_normalize(data_collected)
        table_id = f"{project_id}.{dataset_id}.{entity_name}_{date_str}"
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        print(f"✅ Uploaded {len(df)} records to BigQuery: {table_id}")
    else:
        print(f"⚠️ No {entity_name} found.")

# 🔁 Run for both Receipts and Shifts
pull_and_upload("receipts", RECEIPT_URL, "receipts")
pull_and_upload("shifts", SHIFT_URL, "shifts")
