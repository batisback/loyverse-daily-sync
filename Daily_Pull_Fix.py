import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery

# 🔐 Loyverse API setup
LOYVERSE_TOKEN = "53dbaaeae21541fb89080b0688fc0969"
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

# 📆 Define PH time range for previous full day
today_ph = datetime.now()
start_ph = (today_ph - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
end_ph = start_ph.replace(hour=23, minute=59, second=59, microsecond=999999)

# Convert to UTC
utc_start = (start_ph - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
utc_end = (end_ph - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
date_str = start_ph.strftime("%Y_%m_%d")  # for table name


def fix_shifts_df(df):
    if "taxes" in df.columns:
        df["taxes"] = df["taxes"].apply(
            lambda x: [int(t) for t in x] if isinstance(x, list) and all(str(t).isdigit() for t in x) else []
        )

    if "cash_movements" in df.columns:
        def fix_movements(x):
            if isinstance(x, list) and all(isinstance(i, dict) for i in x):
                return x
            return []
        df["cash_movements"] = df["cash_movements"].apply(fix_movements)

    return df


def pull_and_upload(entity_name, url, key_name):
    print(f"\n📦 Pulling {entity_name} from {start_ph.strftime('%Y-%m-%d')}...")
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
        df = pd.json_normalize(data_collected, sep="_")

        if entity_name == "shifts":
            df = fix_shifts_df(df)

        table_id = f"{project_id}.{dataset_id}.{entity_name}_{date_str}"
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE",
            autodetect=True,
        )

        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"✅ Uploaded {len(df)} records to BigQuery: {table_id}")
    else:
        print(f"⚠️ No {entity_name} found.")


def merge_into_final(table_type):
    temp_table = f"{dataset_id}.{table_type}_{date_str}"
    final_table = f"{dataset_id}.new_final_{table_type}"
    id_field = "receipt_number" if table_type == "receipts" else "id"

    merge_sql = f"""
        MERGE `{project_id}.{final_table}` AS target
        USING `{project_id}.{temp_table}` AS source
        ON target.{id_field} = source.{id_field}
        WHEN NOT MATCHED THEN
          INSERT ROW
    """

    print(f"\n🔁 Merging {table_type}_{date_str} into new_final_{table_type}...")
    client.query(merge_sql).result()
    print(f"✅ Merge complete for new_final_{table_type}")


# 🔁 Pull & upload + merge both Receipts and Shifts
pull_and_upload("receipts", RECEIPT_URL, "receipts")
merge_into_final("receipts")

pull_and_upload("shifts", SHIFT_URL, "shifts")
merge_into_final("shifts")
