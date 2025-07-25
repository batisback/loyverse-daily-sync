from google.cloud import bigquery
import datetime

client = bigquery.Client()

project_id = "loyverse-anomaly-warehouse"
dataset_id = "loyverse_data"

start_date = datetime.date(2025, 1, 1)
end_date = datetime.date(2025, 4, 30)

for i in range((end_date - start_date).days + 1):
    date_str = (start_date + datetime.timedelta(days=i)).strftime("shifts_%Y_%m_%d")
    src_table = f"`{project_id}.{dataset_id}.{date_str}`"
    dst_table = f"`{project_id}.{dataset_id}.{date_str}_fixed`"

    print(f"🔁 Fixing {date_str}...")

    query = f"""
    CREATE OR REPLACE TABLE {dst_table} AS
    SELECT
      id,
      store_id,
      pos_device_id,
      opened_at,
      closed_at,
      opened_by_employee,
      closed_by_employee,
      starting_cash,
      cash_payments,
      cash_refunds,
      paid_in,
      paid_out,
      expected_cash,
      actual_cash,
      gross_sales,
      refunds,
      discounts,
      net_sales,
      tip,
      surcharge,
      taxes,
      payments,
      ARRAY(SELECT TO_JSON_STRING(x) FROM UNNEST(cash_movements) AS x) AS cash_movements
    FROM {src_table}
    """

    try:
        client.query(query).result()
        print(f"✅ Done fixing {date_str}")
    except Exception as e:
        print(f"❌ Error fixing {date_str}: {e}")
