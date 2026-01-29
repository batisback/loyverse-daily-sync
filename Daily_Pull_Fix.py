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
# 🛠️ HELPER FUNCTIONS
# ==============================================================================

def now_ph():
    # GitHub runners are usually UTC; PH is UTC+8
    return datetime.utcnow() + timedelta(hours=8)

def ph_to_utc_z(dt_ph):
    dt_utc = dt_ph - timedelta(hours=8)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%S") + "Z"

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
    client.query(merge_sql).result()
    print(f"✅ Merge complete for new_final_{table_type}")

# ==============================================================================
# 🚀 MAIN EXECUTION (10AM / 10PM PH rolling window)
# Pull from yesterday 00:00 → today 10:00 (AM run) OR today 22:00 (PM run)
# ==============================================================================

ph_now = now_ph()

today_00 = ph_now.replace(hour=0, minute=0, second=0, microsecond=0)
yesterday_00 = today_00 - timedelta(days=1)

# Decide whether this is AM or PM run based on PH time at runtime.
# If you schedule exactly 10AM and 10PM PH, this will behave correctly.
if ph_now.hour < 16:
    cutoff_ph = today_00.replace(hour=10, minute=0, second=0, microsecond=0)
    run_label = "10AM"
else:
    cutoff_ph = today_00.replace(hour=22, minute=0, second=0, microsecond=0)
    run_label = "10PM"

# If job runs earlier than the cutoff time, clamp to now (so you still pull "up to now")
if cutoff_ph > ph_now:
    cutoff_ph = ph_now

utc_start = ph_to_utc_z(yesterday_00)
utc_end = ph_to_utc_z(cutoff_ph)

print(f"\n==================== {run_label} RUN ====================")
print(f"PH Window : {yesterday_00} → {cutoff_ph}")
print(f"UTC Window: {utc_start} → {utc_end}")

# Use a rolling temp table name so reruns overwrite safely
date_str = "rolling_window"

if pull_and_upload("receipts", RECEIPT_URL, "receipts", utc_start, utc_end, date_str):
    merge_into_final("receipts", date_str)

if pull_and_upload("shifts", SHIFT_URL, "shifts", utc_start, utc_end, date_str):
    merge_into_final("shifts", date_str)

print("\n✅ Rolling window sync complete! ✅")
