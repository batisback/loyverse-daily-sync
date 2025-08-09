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

TARGET_STORES = {
    'CHH SB Co': '0c2650c7-6891-445f-bfb6-ad9a996512de', 'S4': '82034dd5-a404-43b4-9b2d-674e3bab0242',
    'SB Co Ebloc3': '9d4f5c35-b777-4382-975f-33d72a1ac476', 'UC Med SB': 'b0baf2e5-2b71-41b6-a5f3-b280c43dc4e3',
    'Sky1 SB': '1153f4fd-c8a7-495f-8a9f-2c6c8aa2cc6e', 'Alpha Simply': '20c5e48d-dc7c-4ad0-8f3f-f833338e5284',
    'Alliance Simply': 'b9b66c09-dbfe-4ea4-9489-05b3320ddce2', 'Skyrise 3': 'f4bf31ef-12a8-4758-b941-8b8be11d7c23',
    'JY SB': 'a254eb83-5e8b-4ff6-86f9-a26d47114288'
}
target_store_ids = list(TARGET_STORES.values())

# --- 2. DATA FETCHING AND PROCESSING ---

def fetch_and_process_data():
    """Fetches and processes shift data for all target stores."""
    print(f"📦 Pulling shift data for the last {BASELINE_DAYS} days...")
    shift_sales_query = f"""
    WITH ShiftSales AS (
        SELECT s.store_id, s.opened_at,
               COALESCE((SELECT SUM(CAST(JSON_EXTRACT_SCALAR(p, '$.money_amount') AS FLOAT64)) FROM UNNEST(s.payments) AS p), 0) AS total_sales
        FROM `{PROJECT_ID}.loyverse_data.new_final_shifts` AS s
        WHERE s.store_id IN ({", ".join([f"'{id}'" for id in target_store_ids])})
          AND s.opened_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {BASELINE_DAYS} DAY)
    )
    SELECT store_id, opened_at AS shift_opening_time, total_sales,
           EXTRACT(DAYOFWEEK FROM opened_at AT TIME ZONE 'Asia/Manila') AS day_of_week,
           EXTRACT(HOUR FROM opened_at AT TIME ZONE 'Asia/Manila') AS hour_of_day
    FROM ShiftSales ORDER BY store_id, shift_opening_time
    """
    all_shifts_df = client.query(shift_sales_query).to_dataframe()

    if all_shifts_df.empty:
        print("❌ No data found for any target stores.")
        return None

    print(f"✅ Found {len(all_shifts_df)} total shifts. Processing...")
    all_shifts_df['shift_opening_time'] = pd.to_datetime(all_shifts_df['shift_opening_time']).dt.tz_convert('Asia/Manila')
    id_to_name_map = {v: k for k, v in TARGET_STORES.items()}
    all_shifts_df['store_name'] = all_shifts_df['store_id'].map(id_to_name_map)
    day_map = {1: 'Sun', 2: 'Mon', 3: 'Tue', 4: 'Wed', 5: 'Thu', 6: 'Fri', 7: 'Sat'}
    all_shifts_df['day_name'] = all_shifts_df['day_of_week'].map(day_map)
    def categorize_shift(hour):
        if 4 <= hour <= 11: return 'AM'
        elif 16 <= hour <= 23: return 'PM'
        else: return 'Other'
    all_shifts_df['time_slot'] = all_shifts_df['hour_of_day'].apply(categorize_shift)
    all_shifts_df['shift_slot'] = all_shifts_df['day_name'] + '-' + all_shifts_df['time_slot']
    return all_shifts_df[all_shifts_df['time_slot'] != 'Other'].copy()

# --- 3. MAIN EXECUTION BLOCK ---

def main():
    all_data = fetch_and_process_data()
    if all_data is None:
        print("Halting execution as no data was found.")
        return

    all_results = []

    for store_name in sorted(all_data['store_name'].unique()):
        print(f"\nANALYZING STORE: {store_name.upper()}")
        store_df = all_data[all_data['store_name'] == store_name].copy()
        
        analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
        baseline_df = store_df[store_df['shift_opening_time'] < analysis_period_start]
        recent_shifts_df = store_df[store_df['shift_opening_time'] >= analysis_period_start].copy()
        
        if recent_shifts_df.empty:
            print(f"No recent shifts found for {store_name}. Skipping.")
            continue

        shift_baselines = baseline_df.groupby('shift_slot')['total_sales'].mean().reset_index()
        shift_baselines.rename(columns={'total_sales': 'avg_historical_sales'}, inplace=True)

        # --- Generate all 3 analyses for the store ---
        # 1. Overall Performance
        total_recent = recent_shifts_df['total_sales'].sum()
        total_baseline = baseline_df['total_sales'].sum()
        num_baseline_weeks = (BASELINE_DAYS - ANALYSIS_DAYS) / 7.0
        avg_weekly_baseline = total_baseline / num_baseline_weeks if num_baseline_weeks > 0 else 0
        
        # 2. Granular Performance
        performance_df = pd.merge(recent_shifts_df, shift_baselines, on='shift_slot', how='left').fillna(0)
        performance_df['sales_difference'] = performance_df['total_sales'] - performance_df['avg_historical_sales']
        performance_df['performance_pct'] = np.divide(performance_df['sales_difference'], performance_df['avg_historical_sales'], out=np.zeros_like(performance_df['sales_difference']), where=performance_df['avg_historical_sales']!=0) * 100
        
        # 3. Consecutive Dips
        performance_df['is_dip'] = performance_df['sales_difference'] < 0
        performance_df['dip_block'] = (performance_df['is_dip'] != performance_df['is_dip'].shift()).cumsum()
        performance_df['consecutive_dip_count'] = performance_df.groupby('dip_block')['is_dip'].transform('size')
        performance_df['alert_consecutive_dip'] = (performance_df['is_dip'] == True) & (performance_df['consecutive_dip_count'] >= 3)

        # --- Format results for this store and add to the main list ---
        overall_summary = f"Overall: {(total_recent - avg_weekly_baseline):+,.2f} ({((total_recent - avg_weekly_baseline)/avg_weekly_baseline)*100:+,.2f}%)"
        
        # Create a temporary row for the overall summary
        summary_row = pd.DataFrame([{'store_name': store_name, 'shift_slot': overall_summary}])
        all_results.append(summary_row)
        
        # Add granular results
        granular_results = performance_df[['store_name', 'shift_opening_time', 'shift_slot', 'sales_difference', 'performance_pct', 'alert_consecutive_dip']]
        all_results.append(granular_results)

    if not all_results:
        print("No results generated.")
        return

    # Combine all results into one big DataFrame
    final_df = pd.concat(all_results, ignore_index=True)

    # --- Write to Google Sheets ---
    print("\nWriting all results to Google Sheets...")
    try:
        sh = gc.open(SPREADSHEET_NAME)
        print(f"📖 Opened existing spreadsheet: '{SPREADSHEET_NAME}'")
    except gspread.exceptions.SpreadsheetNotFound:
        sh = gc.create(SPREADSHEET_NAME)
        print(f"✨ Created new spreadsheet: '{SPREADSHEET_NAME}'")
        sa_email = creds.service_account_email
        sh.share(sa_email, perm_type='user', role='writer')
        print(f"📧 Shared sheet with Service Account: {sa_email}")

    try:
        worksheet = sh.worksheet("Performance Report")
    except gspread.exceptions.WorksheetNotFound:
        worksheet = sh.add_worksheet(title="Performance Report", rows="2000", cols="30")
        
    # Prepare DataFrame for upload
    final_df['shift_opening_time'] = pd.to_datetime(final_df['shift_opening_time']).dt.strftime('%Y-%m-%d %H:%M')
    worksheet.clear()
    set_with_dataframe(worksheet, final_df, include_index=False, allow_formulas=False)
    print("✅ Successfully wrote all performance data to the 'Performance Report' tab.")
    print(f"\n🎉 All tasks complete. View your report at: https://docs.google.com/spreadsheets/d/{sh.id}")

if __name__ == "__main__":
    main()
