from google.cloud import bigquery
from datetime import datetime, timedelta

project_id = "loyverse-anomaly-warehouse"
dataset_id = "loyverse_data"
client = bigquery.Client(project=project_id)

start_date = datetime(2025, 5, 1)
end_date = datetime(2025, 5, 30)

def merge_shifts_for_date(date_obj):
    table_suffix = date_obj.strftime("%Y_%m_%d")
    temp_table = f"{dataset_id}.shifts_{table_suffix}"
    final_table = f"{dataset_id}.final_shifts"

    merge_sql = f"""
        MERGE `{project_id}.{final_table}` AS target
        USING `{project_id}.{temp_table}` AS source
        ON target.id = source.id
        WHEN NOT MATCHED THEN
          INSERT ROW
    """

    print(f"🔁 Merging shifts_{table_suffix} into final_shifts...")
    client.query(merge_sql).result()
    print(f"✅ Done with {table_suffix}")

# 🔁 Run loop from May 1 to May 30
current = start_date
while current <= end_date:
    merge_shifts_for_date(current)
    current += timedelta(days=1)
