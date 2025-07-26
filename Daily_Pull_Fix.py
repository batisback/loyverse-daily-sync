import requests
import pandas as pd
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# 🔐 Loyverse API setup
LOYVERSE_TOKEN = "53dbaaeae21541fb89080b0688fc0969"
HEADERS = {"Authorization": f"Bearer {LOYVERSE_TOKEN}"}
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

# Convert to UTC for API calls
utc_start = (start_ph - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
utc_end = (end_ph - timedelta(hours=8)).strftime("%Y-%m-%dT%H:%M:%S") + "Z"
date_str = start_ph.strftime("%Y_%m_%d")

# ==============================================================================
# ✨ DEFINE STABLE SCHEMAS ✨
# This is the most important change to guarantee stability.
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
    # Storing complex objects as JSON strings is the most flexible approach
    bigquery.SchemaField("line_items", "STRING", mode="REPEATED"),
    bigquery.SchemaField("payments", "STRING", mode="REPEATED"),
    # Other potential fields, ensure they are nullable
    bigquery.SchemaField("note", "STRING"),
    bigquery.SchemaField("customer_id", "STRING"),
    bigquery.SchemaField("employee_id", "STRING"),
    bigquery.SchemaField("store_id", "STRING"),
]

SCHEMA_SHIFTS = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("store_id", "STRING"),
    bigquery.SchemaField("pos_id", "STRING"),
    bigquery.SchemaField("employee_id", "STRING"),
    bigquery.SchemaField("cash_register_id", "STRING"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("opening_time", "TIMESTAMP"),
    bigquery.SchemaField("closing_time", "TIMESTAMP"),
    bigquery.SchemaField("expected_cash_amount", "FLOAT"),
    bigquery.SchemaField("actual_cash_amount", "FLOAT"),
    # Storing complex objects as JSON strings
    bigquery.SchemaField("cash_movements", "STRING", mode="REPEATED"),
    bigquery.SchemaField("payments", "STRING", mode="REPEATED"),
    bigquery.SchemaField("taxes", "STRING", mode="REPEATED"),
]

# Map entity names to their schemas
SCHEMAS = {
    "receipts": SCHEMA_RECEIPTS,
    "shifts": SCHEMA_SHIFTS
}

def create_table_if_not_exists(table_id, schema):
    """Creates a table with a defined schema if it doesn't exist."""
    try:
        client.get_table(table_id) # Check if table exists
        print(f"👍 Table {table_id} already exists.")
    except NotFound:
        print(f"🤔 Table {table_id} not found. Creating it...")
        table = bigquery.Table(table_id, schema=schema)
        client.create_table(table)
        print(f"✅ Table {table_id} created successfully.")


def prepare_dataframe(df, schema):
    """Prepares DataFrame for upload: handles types and ensures all columns exist."""
    import json
    
    # Ensure all columns from the schema exist in the DataFrame, fill missing with None
    for field in schema:
        if field.name not in df.columns:
            df[field.name] = None
        
        # Convert array/object columns to JSON strings
        if field.field_type == 'STRING' and field.mode == 'REPEATED':
             df[field.name] = df[field.name].apply(
                lambda x: [json.dumps(item) for item in x] if isinstance(x, list) else []
            )

    # Convert timestamp columns
    for field in schema:
        if field.field_type == 'TIMESTAMP' and field.name in df.columns:
            df[field.name] = pd.to_datetime(df[field.name], errors='coerce')
            
    return df


def pull_and_upload(entity_name, url, key_name):
    """Pulls data from API and uploads to a daily BigQuery table."""
    print(f"\n📦 Pulling {entity_name} from {start_ph.strftime('%Y-%m-%d')}...")
    # ... (Your existing API pull logic remains the same) ...
    data_collected = []
    params = {"created_at_min": utc_start, "created_at_max": utc_end, "limit": 250}
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

    if not data_collected:
        print(f"⚠️ No {entity_name} found.")
        return

    df = pd.json_normalize(data_collected, sep="_")
    schema = SCHEMAS[entity_name]
    
    # Prepare DataFrame using the schema
    df = prepare_dataframe(df, schema)

    table_id = f"{project_id}.{dataset_id}.{entity_name}_{date_str}"
    job_config = bigquery.LoadJobConfig(
        schema=schema, # Use the explicit schema, not autodetect
        write_disposition="WRITE_TRUNCATE",
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"✅ Uploaded {len(df)} records to BigQuery: {table_id}")


def merge_into_final(table_type):
    """Merges a daily table into the corresponding final table."""
    temp_table_id = f"{project_id}.{dataset_id}.{table_type}_{date_str}"
    final_table_id = f"{project_id}.{dataset_id}.new_final_{table_type}"
    id_field = "receipt_number" if table_type == "receipts" else "id"

    # 1. Create the final table if it doesn't exist
    create_table_if_not_exists(final_table_id, SCHEMAS[table_type])
    
    # 2. Run the merge query
    # The schemas now match perfectly, so INSERT ROW is safe
    merge_sql = f"""
        MERGE `{final_table_id}` AS target
        USING `{temp_table_id}` AS source
        ON target.{id_field} = source.{id_field}
        WHEN NOT MATCHED THEN
          INSERT ROW
    """

    print(f"\n🔁 Merging {table_type}_{date_str} into new_final_{table_type}...")
    try:
        client.query(merge_sql).result()
        print(f"✅ Merge complete for new_final_{table_type}")
    except Exception as e:
        print(f"❌ Merge failed for {table_type}: {e}")
        raise


# ==============================================================================
# 🚀 MAIN EXECUTION
# ==============================================================================
pull_and_upload("receipts", RECEIPT_URL, "receipts")
merge_into_final("receipts")

pull_and_upload("shifts", SHIFT_URL, "shifts")
merge_into_final("shifts")
