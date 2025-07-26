from google.cloud import bigquery
from datetime import datetime, timedelta

project_id = "loyverse-anomaly-warehouse"
dataset_id = "loyverse_data"
client = bigquery.Client(project=project_id)

start_date = datetime(2025, 6, 1)
end_date = datetime(2025, 7, 24) 

def merge_shifts_for_date(date_obj):
    # This line formats the date correctly for the table name
    table_suffix = date_obj.strftime("%Y_%m_%d")
    
    temp_table = f"{dataset_id}.shifts_{table_suffix}"
    final_table = f"{dataset_id}.final_shifts"

    # This SQL query is designed to be robust against all schema errors you have encountered.
    # It transforms data on-the-fly to match the final table's schema.
    merge_sql = f"""
        MERGE `{project_id}.{final_table}` AS target
        USING (
          SELECT
            * EXCEPT (cash_movements, taxes),
            
            -- CRITICAL: This line safely converts the 'cash_movements' column to an array of strings.
            COALESCE((SELECT ARRAY_AGG(TO_JSON_STRING(c)) FROM UNNEST(cash_movements) AS c), []) AS cash_movements,
            
            -- CRITICAL: This line safely converts the 'taxes' column (whether it's an INT or STRUCT) to an array of strings.
            COALESCE((SELECT ARRAY_AGG(TO_JSON_STRING(t)) FROM UNNEST(taxes) AS t), []) AS taxes
            
          FROM
            `{project_id}.{temp_table}`
        ) AS source
        ON target.id = source.id
        WHEN NOT MATCHED THEN
          INSERT ROW
    """

    print(f"🔁 Merging shifts_{table_suffix} into final_shifts...")
    try:
        client.query(merge_sql).result(timeout=300) 
        print(f"✅ Done with {table_suffix}")
    except Exception as e:
        print(f"❌ Error merging {table_suffix}: {e}")
        raise

# 🔁 Run loop from start_date to end_date
current = start_date
while current <= end_date:
    merge_shifts_for_date(current)
    current += timedelta(days=1)
