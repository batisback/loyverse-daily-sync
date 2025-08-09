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

print("🔑 Authenticating...")
scopes = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/bigquery'
]
creds, _ = default(scopes=scopes)
gc = gspread.authorize(creds)
client = bigquery.Client(credentials=creds, project=PROJECT_ID)
print("✅ Authentication successful.")

store_name_map = {
    '0c2650c7-6891-445f-bfb6-ad9a996512de': 'CHH SB Co', '82034dd5-a404-43b4-9b2d-674e3bab0242': 'S4',
    '9d4f5c35-b777-4382-975f-33d72a1ac476': 'SB Co Ebloc3', 'a3291a95-e8e7-45c6-9d44-df81b36a1d37': 'Dtan',
    'b0baf2e5-2b71-41b6-a5f3-b280c43dc4e3': 'UC Med SB', 'c9015f99-b75b-4ee9-ac52-d76eb16cab37': 'Sugbo Sentro',
    '1153f4fd-c8a7-495f-8a9f-2c6c8aa2cc6e': 'Sky1 SB', '1a138ff9-d87f-4e82-951d-a0f89dec364d': 'Wipro',
    '20c5e48d-dc7c-4ad0-8f3f-f833338e5284': 'Alpha Simply', 'b9b66c09-dbfe-4ea4-9489-05b3320ddce2': 'Alliance Simply',
    '61928236-425a-40c8-9b21-38eafd0115f1': 'Trial Store 2', 'f4bf31ef-12a8-4758-b941-8b8be11d7c23': 'Skyrise 3',
    'a254eb83-5e8b-4ff6-86f9-a26d47114288': 'JY SB'
}

# --- 2. ANALYSIS FUNCTIONS ---

def get_processed_shift_data(client, store_map):
    """Fetches and processes the base shift data needed for multiple analyses."""
    print("📦 Pulling and processing base shift data...")
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
    if shift_data.empty: return None
    
    shift_data['store_name'] = shift_data['store_id'].map(store_map).fillna('Unknown Store')
    day_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
    shift_data['day_name'] = shift_data['day_of_week'].map(day_map)
    def categorize_shift(hour):
        if 4 <= hour <= 11: return 'AM'
        elif 16 <= hour <= 23: return 'PM'
        else: return 'Other'
    shift_data['time_slot'] = shift_data['hour_of_day'].apply(categorize_shift)
    shift_data['shift_slot'] = shift_data['day_name'] + '-' + shift_data['time_slot']
    shift_data['shift_opening_time'] = pd.to_datetime(shift_data['shift_opening_time']).dt.tz_convert('Asia/Manila')
    return shift_data[shift_data['time_slot'] != 'Other'].copy()

def generate_sales_dip_anomalies(all_shift_data):
    """Original anomaly detection for sales dips."""
    print("📊 Generating 'Sales Dip Anomalies' report...")
    baselines = all_shift_data.groupby(['store_name', 'shift_slot'])['total_sales'].agg(['mean', 'std']).reset_index()
    baselines.rename(columns={'mean': 'avg_sales', 'std': 'std_sales'}, inplace=True)
    baselines['std_sales'] = baselines['std_sales'].fillna(0)
    analysis_df = pd.merge(all_shift_data, baselines, on=['store_name', 'shift_slot'], how='left')
    
    analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
    recent_shifts_df = analysis_df[analysis_df['shift_opening_time'] >= analysis_period_start].copy()
    
    recent_shifts_df['anomaly_threshold'] = recent_shifts_df['avg_sales'] - (1.8 * recent_shifts_df['std_sales'])
    recent_shifts_df['is_stat_anomaly'] = (recent_shifts_df['total_sales'] < recent_shifts_df['anomaly_threshold']) & (recent_shifts_df['std_sales'] > 0)
    recent_shifts_df['is_hard_rule_anomaly'] = recent_shifts_df['total_sales'] < (recent_shifts_df['avg_sales'] * 0.6)
    recent_shifts_df['is_suspicious'] = recent_shifts_df['is_stat_anomaly'] | recent_shifts_df['is_hard_rule_anomaly']
    
    anomalies = recent_shifts_df[recent_shifts_df['is_suspicious']].copy()
    anomalies.sort_values(by=['store_name', 'shift_opening_time'], inplace=True)
    print(f"🚨 Found {len(anomalies)} suspicious sales dip shifts.")
    return anomalies

def generate_ratio_anomalies(client, store_map):
    """Anomaly detection for Americano/Spanish Latte ratios AND Spanish Latte sales dips."""
    print("📊 Generating 'Ratio Anomalies' report...")
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
    
    pivot_df = data.groupby(["shift_number", "shift_opening_time", "store_id", "name"])["quantity"].sum().unstack(fill_value=0).reset_index()
    if 'Americano' not in pivot_df.columns: pivot_df['Americano'] = 0
    if 'Spanish Latte' not in pivot_df.columns: pivot_df['Spanish Latte'] = 0
    pivot_df['store_name'] = pivot_df['store_id'].map(store_map).fillna('Unknown Store')
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

    # --- UPDATED: Calculate baselines for BOTH Americano and Spanish Latte ---
    baselines = analysis_df.groupby(['store_name', 'shift_slot'])[['Americano', 'Spanish Latte']].agg(['mean', 'std']).reset_index()
    baselines.columns = ['_'.join(col).strip('_') for col in baselines.columns.values] # Flatten multi-index columns
    baselines = baselines.rename(columns={
        'Americano_mean': 'avg_americano', 'Spanish Latte_mean': 'avg_spanish_latte',
        'Spanish Latte_std': 'std_spanish_latte'
    })
    baselines['std_spanish_latte'] = baselines['std_spanish_latte'].fillna(0)

    analysis_df = pd.merge(analysis_df, baselines, on=['store_name', 'shift_slot'], how='left')
    
    analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
    recent_shifts_df = analysis_df[analysis_df['shift_opening_time'] >= analysis_period_start].copy()
    
    # --- Anomaly Condition 1: High Americano vs. Spanish Latte Ratio ---
    recent_shifts_df['americano_vs_spanish_latte_pct'] = np.where(recent_shifts_df['Spanish Latte'] > 0, (recent_shifts_df['Americano'] / recent_shifts_df['Spanish Latte']), np.nan)
    recent_shifts_df.loc[(recent_shifts_df['Spanish Latte'] == 0) & (recent_shifts_df['Americano'] > 0), 'americano_vs_spanish_latte_pct'] = 9.99
    recent_shifts_df['is_ratio_anomaly'] = recent_shifts_df['americano_vs_spanish_latte_pct'] > 0.6

    # --- Anomaly Condition 2: Low Spanish Latte Sales ---
    recent_shifts_df['latte_anomaly_threshold'] = recent_shifts_df['avg_spanish_latte'] - (1.8 * recent_shifts_df['std_spanish_latte'])
    recent_shifts_df['is_latte_dip_anomaly'] = (recent_shifts_df['Spanish Latte'] < recent_shifts_df['latte_anomaly_threshold']) & (recent_shifts_df['std_spanish_latte'] > 0)
    
    # --- Combine conditions and assign a reason for the flag ---
    anomalies = recent_shifts_df[recent_shifts_df['is_ratio_anomaly'] | recent_shifts_df['is_latte_dip_anomaly']].copy()
    
    def get_reason(row):
        reasons = []
        if row['is_ratio_anomaly']:
            reasons.append("High Americano Ratio")
        if row['is_latte_dip_anomaly']:
            reasons.append("Low Spanish Latte Sales")
        return ", ".join(reasons)
        
    if not anomalies.empty:
        anomalies['Reason'] = anomalies.apply(get_reason, axis=1)

    anomalies.sort_values(by=['store_name', 'shift_opening_time'], inplace=True)
    print(f"🚨 Found {len(anomalies)} suspicious ratio/dip shifts.")
    return anomalies

def generate_summary_sales(all_shift_data):
    # ... (This function remains exactly the same) ...
    print("📊 Generating 'Summary Sales' report...")
    analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
    baseline_df = all_shift_data[all_shift_data['shift_opening_time'] < analysis_period_start]
    recent_df = all_shift_data[all_shift_data['shift_opening_time'] >= analysis_period_start]
    recent_sales = recent_df.groupby('store_name')['total_sales'].sum().reset_index()
    recent_sales.rename(columns={'total_sales': 'Sales Amount (Past 7 Days)'}, inplace=True)
    historic_sales = baseline_df.groupby('store_name')['total_sales'].sum().reset_index()
    num_baseline_weeks = (BASELINE_DAYS - ANALYSIS_DAYS) / 7.0
    historic_sales['Sales Average per 7 Days'] = historic_sales['total_sales'] / num_baseline_weeks if num_baseline_weeks > 0 else 0
    summary_df = pd.merge(recent_sales, historic_sales[['store_name', 'Sales Average per 7 Days']], on='store_name', how='left')
    summary_df.rename(columns={'store_name': 'Branch'}, inplace=True)
    return summary_df

def generate_granular_sales(all_shift_data):
    # ... (This function remains exactly the same) ...
    print("📊 Generating 'Granular Sales' report...")
    analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
    baseline_df = all_shift_data[all_shift_data['shift_opening_time'] < analysis_period_start]
    recent_df = all_shift_data[all_shift_data['shift_opening_time'] >= analysis_period_start]
    historic_shift_avg = baseline_df.groupby(['store_name', 'shift_slot'])['total_sales'].mean().reset_index()
    historic_shift_avg.rename(columns={'total_sales': 'Average Historic Sales'}, inplace=True)
    details_df = pd.merge(recent_df, historic_shift_avg, on=['store_name', 'shift_slot'], how='left')
    details_df = details_df[['store_name', 'shift_slot', 'total_sales', 'Average Historic Sales']].copy()
    details_df.rename(columns={'store_name': 'Branch Name', 'shift_slot': 'Shift', 'total_sales': 'Actual Sales'}, inplace=True)
    return details_df

# --- 3. MAIN EXECUTION BLOCK ---

def write_to_sheet(spreadsheet, sheet_name, df):
    """Helper function to write a DataFrame to a specified worksheet."""
    if df is None or df.empty:
        print(f"⚠️ No data to write for '{sheet_name}'. Skipping.")
        return
        
    for col in df.select_dtypes(include=['datetimetz', 'datetime64[ns, Asia/Manila]']):
        df[col] = df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols=len(df.columns) + 2) # Added +2 for new columns
        
    worksheet.clear()
    set_with_dataframe(worksheet, df)
    print(f"✅ Successfully wrote data to '{sheet_name}' tab.")

def main():
    try:
        sh = gc.open(SPREADSHEET_NAME)
        print(f"📖 Opened existing spreadsheet: '{SPREADSHEET_NAME}'")
    except gspread.exceptions.SpreadsheetNotFound:
        sh = gc.create(SPREADSHEET_NAME)
        print(f"✨ Created new spreadsheet: '{SPREADSHEET_NAME}'")
        sa_email = creds.service_account_email
        sh.share(sa_email, perm_type='user', role='writer')
        print(f"📧 Shared sheet with Service Account: {sa_email}")

    # --- Generate all reports ---
    base_shift_data = get_processed_shift_data(client, store_name_map)
    
    if base_shift_data is not None:
        sales_dip_anomalies_df = generate_sales_dip_anomalies(base_shift_data)
        summary_sales_df = generate_summary_sales(base_shift_data)
        granular_sales_df = generate_granular_sales(base_shift_data)
    else:
        print("Halting shift-based analyses as no base data was found.")
        sales_dip_anomalies_df = pd.DataFrame()
        summary_sales_df = pd.DataFrame()
        granular_sales_df = pd.DataFrame()
    
    ratio_anomalies_df = generate_ratio_anomalies(client, store_name_map)

    # --- Write all reports to Google Sheets ---
    print("\n📝 Writing all results to Google Sheets...")
    write_to_sheet(sh, "Sales Dip Anomalies", sales_dip_anomalies_df)
    write_to_sheet(sh, "Ratio Anomalies", ratio_anomalies_df)
    write_to_sheet(sh, "Summary Sales", summary_sales_df)
    write_to_sheet(sh, "Granular Sales", granular_sales_df)

    print(f"\n🎉 All tasks complete. View your report at: https://docs.google.com/spreadsheets/d/{sh.id}")

if __name__ == "__main__":
    main()
