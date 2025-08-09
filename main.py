import pandas as pd
from google.cloud import bigquery
import numpy as np
import os
import gspread
from google.auth import default
from gspread_dataframe import set_with_dataframe

# --- 1. GLOBAL CONFIGURATION ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "loyverse-anomaly-warehouse")
BASELINE_DAYS = 90
ANALYSIS_DAYS = 7
SPREADSHEET_NAME = "loyverse AI Reports"

# --- Authentication (Handles both BigQuery and Sheets) ---
# In GitHub Actions, credentials are automatically found from the environment.
print("🔑 Authenticating...")
from google.auth import default

# --- FIX: Define the necessary scopes for Drive and Sheets ---
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]
creds, _ = default(scopes=scopes)
gc = gspread.authorize(creds)
client = bigquery.Client(credentials=creds, project=PROJECT_ID)
print("✅ Authentication successful with Drive and Sheets permissions.")


# Store name mapping
store_name_map = {
    '0c2650c7-6891-445f-bfb6-ad9a996512de': 'CHH SB Co', '82034dd5-a404-43b4-9b2d-674e3bab0242': 'S4',
    '9d4f5c35-b777-4382-975f-33d72a1ac476': 'SB Co Ebloc3', 'a3291a95-e8e7-45c6-9d44-df81b36a1d37': 'Dtan',
    'b0baf2e5-2b71-41b6-a5f3-b280c43dc4e3': 'UC Med SB', 'c9015f99-b75b-4ee9-ac52-d76eb16cab37': 'Sugbo Sentro',
    '1153f4fd-c8a7-495f-8a9f-2c6c8aa2cc6e': 'Sky1 SB', '1a138ff9-d87f-4e82-951d-a0f89dec364d': 'Wipro',
    '20c5e48d-dc7c-4ad0-8f3f-f833338e5284': 'Alpha Simply', 'b9b66c09-dbfe-4ea4-9489-05b3320ddce2': 'Alliance Simply',
    '61928236-425a-40c8-9b21-38eafd0115f1': 'Trial Store 2', 'f4bf31ef-12a8-4758-b941-8b8be11d7c23': 'Skyrise 3',
    'a254eb83-5e8b-4ff6-86f9-a26d47114288': 'JY SB'
}

# --- 2. ANALYSIS FUNCTIONS (Same as your Colab script) ---

def run_sales_dip_analysis(client, store_name_map):
    """Runs the sales dip analysis and returns a DataFrame of anomalies."""
    print("📊 Running Sales Dip analysis...")
    shift_sales_query = f"""
    WITH ShiftSales AS (
        SELECT s.id AS shift_number, s.opened_at, s.closed_at, s.store_id,
               COALESCE((SELECT SUM(CAST(JSON_EXTRACT_SCALAR(p, '$.money_amount') AS FLOAT64)) FROM UNNEST(s.payments) AS p), 0) AS total_sales
        FROM `{PROJECT_ID}.loyverse_data.new_final_shifts` AS s
        WHERE s.opened_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {BASELINE_DAYS} DAY)
    )
    SELECT shift_number, opened_at AS shift_opening_time, closed_at AS shift_closing_time, store_id, total_sales,
           EXTRACT(DAYOFWEEK FROM opened_at AT TIME ZONE 'Asia/Manila') AS day_of_week,
           EXTRACT(HOUR FROM opened_at AT TIME ZONE 'Asia/Manila') AS hour_of_day
    FROM ShiftSales ORDER BY store_id, shift_opening_time
    """
    shift_data = client.query(shift_sales_query).to_dataframe()
    if shift_data.empty: return pd.DataFrame()
    # ... (rest of the sales dip logic from your Colab script) ...
    shift_data['store_name'] = shift_data['store_id'].map(store_name_map).fillna('Unknown Store')
    day_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
    shift_data['day_name'] = shift_data['day_of_week'].map(day_map)
    def categorize_shift(hour):
        if 4 <= hour <= 11: return 'AM'
        elif 16 <= hour <= 23: return 'PM'
        else: return 'Other'
    shift_data['time_slot'] = shift_data['hour_of_day'].apply(categorize_shift)
    shift_data['shift_slot'] = shift_data['day_name'] + '-' + shift_data['time_slot']
    analysis_data = shift_data[shift_data['time_slot'] != 'Other'].copy()
    baselines = analysis_data.groupby(['store_name', 'shift_slot'])['total_sales'].agg(['mean', 'std']).reset_index()
    baselines.rename(columns={'mean': 'avg_sales', 'std': 'std_sales'}, inplace=True)
    baselines['std_sales'] = baselines['std_sales'].fillna(0)
    analysis_df = pd.merge(analysis_data, baselines, on=['store_name', 'shift_slot'], how='left')
    analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
    analysis_df['shift_opening_time'] = pd.to_datetime(analysis_df['shift_opening_time'])
    recent_shifts_df = analysis_df[analysis_df['shift_opening_time'] >= analysis_period_start].copy()
    recent_shifts_df['anomaly_threshold'] = recent_shifts_df['avg_sales'] - (1.8 * recent_shifts_df['std_sales'])
    recent_shifts_df['is_stat_anomaly'] = (recent_shifts_df['total_sales'] < recent_shifts_df['anomaly_threshold']) & (recent_shifts_df['std_sales'] > 0)
    recent_shifts_df['is_hard_rule_anomaly'] = recent_shifts_df['total_sales'] < (recent_shifts_df['avg_sales'] * 0.6)
    recent_shifts_df['is_suspicious'] = recent_shifts_df['is_stat_anomaly'] | recent_shifts_df['is_hard_rule_anomaly']
    anomalies = recent_shifts_df[recent_shifts_df['is_suspicious']].copy()
    anomalies.sort_values(by=['store_name', 'shift_opening_time'], inplace=True)
    print(f"🚨 Found {len(anomalies)} suspicious sales dip shifts.")
    return anomalies

def run_ratio_analysis(client, store_name_map):
    """Runs the ratio analysis and returns a DataFrame of anomalies."""
    print("📊 Running Americano vs. Spanish Latte ratio analysis...")
    query = f"""
    WITH ShiftsInRange AS (
        SELECT id, store_id, opened_at, closed_at FROM `{PROJECT_ID}.loyverse_data.new_final_shifts`
        WHERE opened_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {BASELINE_DAYS} DAY)
          AND (EXTRACT(HOUR FROM opened_at AT TIME ZONE 'Asia/Manila') BETWEEN 4 AND 11 OR EXTRACT(HOUR FROM opened_at AT TIME ZONE 'Asia/Manila') BETWEEN 16 AND 23)
    ),
    ReceiptItemsInRange AS (
        SELECT created_at, store_id, JSON_EXTRACT_SCALAR(item, '$.item_name') AS name, CAST(JSON_EXTRACT_SCALAR(item, '$.quantity') AS FLOAT64) AS quantity
        FROM `{PROJECT_ID}.loyverse_data.new_final_receipts` AS r, UNNEST(r.line_items) AS item
        WHERE created_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {BASELINE_DAYS} DAY)
          AND JSON_EXTRACT_SCALAR(item, '$.item_name') IN ("Americano", "Spanish Latte")
    )
    SELECT s.id AS shift_number, s.opened_at AS shift_opening_time, s.store_id, ri.name, ri.quantity
    FROM ReceiptItemsInRange AS ri JOIN ShiftsInRange AS s ON ri.store_id = s.store_id
    AND ri.created_at BETWEEN s.opened_at AND IFNULL(s.closed_at, CURRENT_TIMESTAMP())
    """
    data = client.query(query).to_dataframe()
    if data.empty: return pd.DataFrame()
    # ... (rest of the ratio analysis logic from your Colab script) ...
    pivot_df = data.groupby(["shift_number", "shift_opening_time", "store_id", "name"])["quantity"].sum().unstack(fill_value=0).reset_index()
    if 'Americano' not in pivot_df.columns: pivot_df['Americano'] = 0
    if 'Spanish Latte' not in pivot_df.columns: pivot_df['Spanish Latte'] = 0
    pivot_df['store_name'] = pivot_df['store_id'].map(store_name_map).fillna('Unknown Store')
    pivot_df['shift_opening_time'] = pd.to_datetime(pivot_df['shift_opening_time']).dt.tz_convert('Asia/Manila')
    day_map = {0: 'Mon', 1: 'Tue', 2: 'Wed', 3: 'Thu', 4: 'Fri', 5: 'Sat', 6: 'Sun'}
    pivot_df['day_name'] = pivot_df['shift_opening_time'].dt.dayofweek.map(day_map)
    def categorize_shift(hour):
        if 4 <= hour <= 11: return 'AM'
        elif 16 <= hour <= 23: return 'PM'
        else: return 'Other'
    pivot_df['time_slot'] = pivot_df['shift_opening_time'].dt.hour.apply(categorize_shift)
    pivot_df['shift_slot'] = pivot_df['day_name'] + '-' + pivot_df['time_slot']
    analysis_df = pivot_df[pivot_df['time_slot'] != 'Other'].copy()
    baselines = analysis_df.groupby(['store_name', 'shift_slot'])['Americano'].mean().reset_index()
    baselines.rename(columns={'Americano': 'avg_americano_orders'}, inplace=True)
    analysis_df = pd.merge(analysis_df, baselines, on=['store_name', 'shift_slot'], how='left')
    analysis_df['avg_americano_orders'] = analysis_df['avg_americano_orders'].fillna(0)
    analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
    recent_shifts_df = analysis_df[analysis_df['shift_opening_time'] >= analysis_period_start].copy()
    recent_shifts_df['americano_vs_spanish_latte_pct'] = np.where(recent_shifts_df['Spanish Latte'] > 0, (recent_shifts_df['Americano'] / recent_shifts_df['Spanish Latte']), np.nan)
    recent_shifts_df.loc[(recent_shifts_df['Spanish Latte'] == 0) & (recent_shifts_df['Americano'] > 0), 'americano_vs_spanish_latte_pct'] = 9.99
    anomalies = recent_shifts_df[recent_shifts_df['americano_vs_spanish_latte_pct'] > 0.6].copy()
    anomalies.sort_values(by=['store_name', 'shift_opening_time'], inplace=True)
    print(f"🚨 Found {len(anomalies)} suspicious ratio shifts.")
    return anomalies

# --- 3. MAIN EXECUTION BLOCK ---

def main():
    try:
        sh = gc.open(SPREADSHEET_NAME)
        print(f"📖 Opened existing spreadsheet: '{SPREADSHEET_NAME}'")
    except gspread.exceptions.SpreadsheetNotFound:
        sh = gc.create(SPREADSHEET_NAME)
        print(f"✨ Created new spreadsheet: '{SPREADSHEET_NAME}'")
        # Find the service account email and share the sheet with it
        sa_email = creds.service_account_email
        sh.share(sa_email, perm_type='user', role='writer')
        print(f"📧 Shared sheet with Service Account: {sa_email}")

    # Process and Upload Sales Dip Anomalies
    sales_dip_df = run_sales_dip_analysis(client, store_name_map)
    if not sales_dip_df.empty:
        for col in sales_dip_df.select_dtypes(include=['datetimetz']): sales_dip_df[col] = sales_dip_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        try:
            worksheet = sh.worksheet("Sales Dip Anomalies")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title="Sales Dip Anomalies", rows="1000", cols="30")
        worksheet.clear(); set_with_dataframe(worksheet, sales_dip_df)
        print("✅ Wrote sales dip data to the sheet.")
    
    # Process and Upload Ratio Anomalies
    ratio_df = run_ratio_analysis(client, store_name_map)
    if not ratio_df.empty:
        for col in ratio_df.select_dtypes(include=['datetimetz']): ratio_df[col] = ratio_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        try:
            worksheet = sh.worksheet("Ratio Anomalies")
        except gspread.exceptions.WorksheetNotFound:
            worksheet = sh.add_worksheet(title="Ratio Anomalies", rows="1000", cols="30")
        worksheet.clear(); set_with_dataframe(worksheet, ratio_df)
        print("✅ Wrote ratio data to the sheet.")

    print(f"\n🎉 All tasks complete. View your report at: https://docs.google.com/spreadsheets/d/{sh.id}")

if __name__ == "__main__":
    main()
