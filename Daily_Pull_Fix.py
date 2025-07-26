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

# Schemas are not used in this diagnostic run but are kept for context
SCHEMA_RECEIPTS = [bigquery.SchemaField("receipt_number", "STRING")] # Truncated for brevity
SCHEMA_SHIFTS = [bigquery.SchemaField("id", "STRING")] # Truncated for brevity
SCHEMAS = {"receipts": SCHEMA_RECEIPTS, "shifts": SCHEMA_SHIFTS}

# ==============================================================================
# 🛠️ DIAGNOSTIC HELPER FUNCTION
# ==============================================================================

# THIS IS A TEMPORARY, DIAGNOSTIC VERSION OF THIS FUNCTION
def pull_and_upload(entity_name, url, key_name, utc_start, utc_end, date_str):
    # This function will only process shifts for this test
    if entity_name != "shifts":
        print(f"Skipping {entity_name} for this diagnostic run.")
        return True # Return True to allow the script to continue to the next step

    print(f"\n🕵️  Fetching raw data for {entity_name} on {date_str}...")
    params = {"created_at_min": utc_start, "created_at_max": utc_end, "limit": 1}
    
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"❌ API Error {response.status_code}: {response.text}")
        return False

    json_data = response.json()
    data_collected = json_data.get(key_name, [])

    if not data_collected:
        print("⚠️ No shifts found for this day.")
        return False
        
    # Print the raw data for the first shift
    print("\n--- Raw Shift Data from API ---")
    print(json.dumps(data_collected[0], indent=2))
    print("\n--- End of Diagnostic ---")
    
    # Return False to stop the script after printing the diagnostic info
    return False

# These functions won't be called but are kept for script integrity
def merge_into_final(table_type, date_str):
    print("Skipping merge for diagnostic run.")

# ==============================================================================
# 🚀 MAIN EXECUTION (FOR DIAGNOSTIC)
# ==============================================================================

# --- Set a SINGLE day for the diagnostic run ---
DIAGNOSTIC_DATE = datetime(2025, 7, 25)

# --- Run for the single diagnostic date ---
date_to_process = DIAGNOSTIC_DATE
print(f"==================== RUNNING DIAGNOSTIC FOR: {date_to_process.strftime('%Y-%m-%d')} ====================")

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
