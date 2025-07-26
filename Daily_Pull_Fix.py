import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import json
import time

# ==============================================================================
# ⚙️ CONFIGURATION
# ==============================================================================
LOYVERSE_TOKEN = "53dbaaeae21541fb89080b0688fc0969"
HEADERS = {"Authorization": f"Bearer {LOYVERSE_TOKEN}"}
RECEIPT_URL = "https://api.loyverse.com/v1.0/receipts"
SHIFT_URL = "https://api.loyverse.com/v1.0/shifts"

project_id = "loyverse-anomaly-warehouse"
dataset_id = "loyverse_data"
client = bigquery.Client(project=project_id)

# ==============================================================================
# ✨ DEFINE STABLE SCHEMAS (WITH CORRECTIONS) ✨
# ==============================================================================
SCHEMA_RECEIPTS = [
    bigquery.SchemaField("receipt_number", "STRING"),
    bigquery.SchemaField("receipt_type", "STRING"),
    bigquery.SchemaField("source", "STRING"),
    bigquery.SchemaField("created_at", "TIMESTAMP"),
    bigquery.SchemaField("updated_at", "TIMESTAMP"),
    bigquery.SchemaField("total_money", "FLOAT"),
    bigquery.SchemaField("total_tax", "FLOAT"),
    bigquery.SchemaField("points_earned", "INTEGER"),
    bigquery.SchemaField("points_spent", "INTEGER"),
    bigquery.SchemaField("points_balance", "INTEGER"),
    bigquery.SchemaField("line_items", "STRING", mode="REPEATED"),
    bigquery.SchemaField("payments", "STRING", mode="REPEATED"),
    bigquery.SchemaField("note", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("employee_id", "STRING"),
    bigquery.SchemaField("store_id", "STRING"),
]

# --- THE FIX IS HERE: Schema updated to match the API response ---
SCHEMA_SHIFTS = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("store_id", "STRING"),
    bigquery.SchemaField("pos_device_id", "STRING"),
    bigquery.SchemaField("opened_by_employee", "STRING"),
    bigquery.SchemaField("closed_by_employee", "STRING"),
    bigquery.SchemaField("opened_at", "TIMESTAMP"),
    bigquery.SchemaField("closed_at", "TIMESTAMP"),
    bigquery.SchemaField("expected_cash", "FLOAT"),
    bigquery.SchemaField("actual_cash", "FLOAT"),
    bigquery.SchemaField("cash_movements", "STRING", mode="REPEATED"),
    bigquery.SchemaField("payments", "STRING", mode="REPEATED"),
    bigquery.SchemaField("taxes", "STRING", mode="REPEATED"),
]

SCHEMAS = {"receipts": SCHEMA_RECEIPTS, "shifts": SCHEMA_SHIFTS}

# ==============================================================================
# 🛠️ HELPER FUNCTIONS (Restored to original versions)
# ==============================================================================
def create_table_if_not_exists(table_id, schema):
    try:
        client.get_table(table_id)
    except NotFound:
        print(f"🤔 Table {table_id} not found. Creating it...")
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"✅ Table {table_id} created successfully.")

def prepare_dataframe(df, schema):
    schema_columns = [field.name for field in schema]
    df = df.reindex(columns=schema_columns)
    for field in schema:
        if field.field_type == 'STRING' and field.mode == 'REPEATED':
            df[field.name] = df[field.name].apply(
                lambda x: [json.dumps(item) for item in x] if isinstance(x, list) else []
            )
        if field.field_type == 'TIMESTAMP':
            df[field.name] = pd.to_datetime(df[field.name], errors='coerce', utc=True)
    return df

def pull_and_upload(entity_name, url, key_name, utc_start, utc_end, date_str):
    print(f"\n📦 Pulling {entity_name} for {date_str}...")
    data_collected = []
    params = {"created_at_min": utc_start, "created_at_max": utc_end, "limit": 250}
    while True:
        response = requests.get(url, headers=HEADERS, params=params)
        if response.status_code != 200:
            print(f"❌ API Error {response.status_code} for {entity_name} on {date_str}: {response.text}")
            return False
        json_data = response.json()
        data_collected.extend(json_data.get(key_name, []))
        next_cursor = json_data.get("cursor")
        if not next_cursor:
            break
        params["cursor"] = next_cursor
        time.sleep(0.5)

    if not data_collected:
        print(f"⚠️ No {entity_name} found for {date_str}.")
        return True

    df = pd.json_normalize(data_collected, sep="_")
    schema = SCHEMAS[entity_name]
    df = prepare_dataframe(df, schema)
    table_id = f"{project_id}.{dataset_id}.{entity_name}_{date_str}"
    job_config = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} {entity_name} records to: {table_id}")
    return True

def merge_into_final(table_type, date_str):
    temp_table_id = f"{project_id}.{dataset_id}.{table_type}_{date_str}"
    final_table_id = f"{project_id}.{dataset_id}.new_final_{table_type}"
    id_field = "receipt_number" if table_type == "receipts" else "id"
    create_table_if_not_exists(final_table_id, SCHEMAS[table_type])
    merge_sql = f"""
        MERGE `{final_table_id}` AS target
        USING `{temp_table_id}` AS source
        ON target.{id_field} = source.{id_field}
        WHEN NOT MATCHED THEN INSERT ROW
        WHEN MATCHED THEN UPDATE SET {', '.join([f'target.{f.name} = source.{f.name}' for f in SCHEMAS[table_type]])}
    """
    print(f"🔁 Merging {table_type}_{date_str} into new_final_{table_type}...")
    try:
        client.query(merge_sql).result()
        print(f"✅ Merge complete for new_final_{table_type}")
    except Exception as e:
        print(f"❌ Merge failed for {table_type} on {date_str}: {e}")
        raise

# ==============================================================================
# 🚀 MAIN EXECUTION
# ==============================================================================

# --- Define the date range for the TEST backfill ---
BACKFILL_START_DATE = datetime(2025, 7, 1)
BACKFILL_END_DATE = datetime(2025, 7, 23)

# --- Loop through each day in the range ---
current_date = BACKFILL_START_DATE
while current_date <= BACKFILL_END_DATE:
    date_to_process = current_date
    print(f"\n==================== PROCESSING DATE: {date_to_process.strftime('%Y-%m-%d')} ====================")

    # Define the time window for the current day in the loop
    start_ph = date_to_process.replace(hour=0, minute=0, second=0, microsecond=0)
    end_ph = start_ph.replace(hour=23, minute=59, second=59, microsecond=999999)
    utc_start = (start_ph - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    utc_end = (end_ph - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    date_str = start_ph.strftime("%Y_%m_%d")

    # Run the process for the current day
    if pull_and_upload("receipts", RECEIPT_URL, "receipts", utc_start, utc_end, date_str):
        merge_into_final("receipts", date_str)
    
    if pull_and_upload("shifts", SHIFT_URL, "shifts", utc_start, utc_end, date_str):
        merge_into_final("shifts", date_str)

    # Move to the next day
    current_date += timedelta(days=1)

print("\n\n🎉🎉🎉 Test Backfill Complete! 🎉🎉🎉")
```
