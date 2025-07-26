from google.cloud import bigquery
from datetime import datetime, timedelta

project_id = "loyverse-anomaly-warehouse"
dataset_id = "loyverse_data"
client = bigquery.Client(project=project_id)

start_date = datetime(2025, 6, 1)
# Corrected the end date from the original script to match the log files
end_date = datetime(2025, 7, 24) 

def merge_shifts_for_date(date_obj):
    table_suffix = date_obj.strftime("%Y_%m_%d")
    temp_table = f"{dataset_id}.shifts_{table_suffix}"
    final_table = f"{dataset_id}.final_shifts"

    # Robust MERGE statement to handle multiple data type mismatches
    merge_sql = f"""
        MERGE `{project_id}.{final_table}` AS target
        USING (
          -- This subquery transforms multiple source columns to match the target schema
          SELECT
            * EXCEPT (cash_movements, taxes),
            -- Fix for cash_movements (STRUCT -> STRING)
            (SELECT ARRAY_AGG(TO_JSON_STRING(c)) FROM UNNEST(cash_movements) AS c) AS cash_movements,
            -- Fix for taxes (INT64 -> STRING)
            (SELECT ARRAY_AGG(CAST(t AS STRING)) FROM UNNEST(taxes) AS t) AS taxes
          FROM
            `{project_id}.{temp_table}`
        ) AS source
        ON target.id = source.id
        WHEN NOT MATCHED THEN
          INSERT ROW
    """

    print(f"🔁 Merging shifts_{table_suffix} into final_shifts...")
    try:
        # Note: A timeout is added for extra stability on long-running jobs
        client.query(merge_sql).result(timeout=300) 
        print(f"✅ Done with {table_suffix}")
    except Exception as e:
        print(f"❌ Error merging {table_suffix}: {e}")
        raise

# 🔁 Run loop
current = start_date
while current <= end_date:
    merge_shifts_for_date(current)
    current += timedelta(days=1)
