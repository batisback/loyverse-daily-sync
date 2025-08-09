import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns
import IPython.display as dp
import numpy as np

# --- 1. Configuration ---
PROJECT_ID = "loyverse-anomaly-warehouse"

# Define the list of stores you want to analyze
TARGET_STORES = {
    'CHH SB Co': '0c2650c7-6891-445f-bfb6-ad9a996512de', 'S4': '82034dd5-a404-43b4-9b2d-674e3bab0242',
    'SB Co Ebloc3': '9d4f5c35-b777-4382-975f-33d72a1ac476', 'UC Med SB': 'b0baf2e5-2b71-41b6-a5f3-b280c43dc4e3',
    'Sky1 SB': '1153f4fd-c8a7-495f-8a9f-2c6c8aa2cc6e', 'Alpha Simply': '20c5e48d-dc7c-4ad0-8f3f-f833338e5284',
    'Alliance Simply': 'b9b66c09-dbfe-4ea4-9489-05b3320ddce2', 'Skyrise 3': 'f4bf31ef-12a8-4758-b941-8b8be11d7c23',
    'JY SB': 'a254eb83-5e8b-4ff6-86f9-a26d47114288'
}
target_store_ids = list(TARGET_STORES.values())

BASELINE_DAYS = 90
ANALYSIS_DAYS = 7

client = bigquery.Client(project=PROJECT_ID)
print(f"✅ Configuration loaded. Analyzing {len(TARGET_STORES)} stores.")

# --- 2. BigQuery Query ---
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

# --- 3. Data Fetching and Processing ---
print(f"📦 Pulling shift sales data for all stores for the last {BASELINE_DAYS} days...")
all_shifts_df = client.query(shift_sales_query).to_dataframe()

if all_shifts_df.empty:
    print(f"❌ No sales data found for any of the target stores in the last {BASELINE_DAYS} days.")
else:
    print(f"✅ Found {len(all_shifts_df)} total shifts across all stores.")

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
    analysis_df_all_stores = all_shifts_df[all_shifts_df['time_slot'] != 'Other'].copy()

    # --- 4. Main Loop: Analyze Each Store Separately ---
    for store_name in sorted(analysis_df_all_stores['store_name'].unique()):
        
        print("\n\n" + "="*70)
        print(f"          ANALYZING STORE: {store_name.upper()}")
        print("="*70)

        store_df = analysis_df_all_stores[analysis_df_all_stores['store_name'] == store_name].copy()
        analysis_period_start = pd.Timestamp.now(tz='Asia/Manila') - pd.Timedelta(days=ANALYSIS_DAYS)
        baseline_df = store_df[store_df['shift_opening_time'] < analysis_period_start]
        recent_shifts_df = store_df[store_df['shift_opening_time'] >= analysis_period_start].copy()
        
        if recent_shifts_df.empty:
            print(f"No recent shifts found for {store_name} in the last {ANALYSIS_DAYS} days. Skipping.")
            continue

        shift_baselines = baseline_df.groupby('shift_slot')['total_sales'].mean().reset_index()
        shift_baselines.rename(columns={'total_sales': 'avg_historical_sales'}, inplace=True)

        # --- Overall Weekly Performance ---
        print(f"\n## 1. Overall Weekly Performance: '{store_name}'")
        total_recent_sales = recent_shifts_df['total_sales'].sum()
        total_baseline_sales = baseline_df['total_sales'].sum()
        num_baseline_weeks = (BASELINE_DAYS - ANALYSIS_DAYS) / 7.0
        avg_weekly_baseline_sales = total_baseline_sales / num_baseline_weeks if num_baseline_weeks > 0 else 0

        if avg_weekly_baseline_sales > 0:
            sales_difference = total_recent_sales - avg_weekly_baseline_sales
            overall_pct_change = (sales_difference / avg_weekly_baseline_sales) * 100
            change_direction = "increase" if overall_pct_change >= 0 else "decrease"
            print(f"\nComparing the last {ANALYSIS_DAYS} days to the previous {BASELINE_DAYS - ANALYSIS_DAYS} days:\n")
            print(f"  > A {abs(overall_pct_change):.2f}% {change_direction} in overall sales.")
            print(f"  > ₱{sales_difference:,.2f} {change_direction} from the {BASELINE_DAYS}-day average.")
        else:
            print("\n  > Not enough historical data to calculate overall weekly performance.")

        # --- Granular Shift Performance ---
        print(f"\n## 2. Granular Shift Performance: '{store_name}' (Last 7 Days)\n")
        performance_df = pd.merge(recent_shifts_df, shift_baselines, on='shift_slot', how='left')
        performance_df['avg_historical_sales'] = performance_df['avg_historical_sales'].fillna(0)
        performance_df['sales_difference'] = performance_df['total_sales'] - performance_df['avg_historical_sales']
        performance_df['performance_pct'] = np.divide(
            performance_df['sales_difference'], performance_df['avg_historical_sales'],
            out=np.zeros_like(performance_df['sales_difference']), where=performance_df['avg_historical_sales']!=0
        ) * 100
        
        summary_table = performance_df[['shift_opening_time', 'shift_slot', 'sales_difference', 'performance_pct']].sort_values(by='shift_opening_time')
        summary_table.rename(columns={'sales_difference': 'Sales Difference (₱)'}, inplace=True)
        display(summary_table.style.format({
            'shift_opening_time': '{:%a, %b %d - %I:%M %p}', 'Sales Difference (₱)': '{:+,.2f}', 'performance_pct': '{:+.2f}%'
        }).set_caption(f"Shift Performance for {store_name} vs. Historical Average"))
        
        # --- Visualization ---
        plt.style.use('seaborn-v0_8-whitegrid')
        fig, ax = plt.subplots(figsize=(15, 7))
        colors = ['#28a745' if x >= 0 else '#dc3545' for x in summary_table['performance_pct']]
        sns.barplot(data=summary_table, x='shift_slot', y='performance_pct', hue='shift_slot', palette=colors, ax=ax, legend=False)
        ax.set_title(f"Shift Performance vs. Historic Average for '{store_name}'", fontsize=16, weight='bold'); ax.set_xlabel("Shift Slot", fontsize=12)
        ax.set_ylabel("Performance (% Change from Average)", fontsize=12); ax.axhline(0, color='black', linewidth=0.8)
        ax.tick_params(axis='x', rotation=45)
        for p in ax.patches:
            ax.annotate(f'{p.get_height():+.1f}%', (p.get_x() + p.get_width() / 2., p.get_height()), 
                       ha='center', va='center', xytext=(0, 9 if p.get_height() >=0 else -9), 
                       textcoords='offset points', fontsize=10)
        plt.tight_layout(); plt.show()
        
        # --- NEW: 3. Consecutive Sales Dips Alert ---
        print(f"\n## 3. Consecutive Sales Dips Alert: '{store_name}'")
        
        # Determine which shifts are dips (sales < average)
        performance_df['is_dip'] = performance_df['total_sales'] < performance_df['avg_historical_sales']
        
        # This clever trick identifies blocks of consecutive same values
        performance_df['dip_block'] = (performance_df['is_dip'] != performance_df['is_dip'].shift()).cumsum()
        
        # Now, for each row, count how many shifts are in its block
        performance_df['consecutive_dip_count'] = performance_df.groupby('dip_block')['is_dip'].transform('size')
        
        # Filter for the shifts that are part of a dip block of 3 or more
        consecutive_dips_df = performance_df[
            (performance_df['is_dip'] == True) & 
            (performance_df['consecutive_dip_count'] >= 3)
        ].copy()
        
        if not consecutive_dips_df.empty:
            print("\n🚨 ALERT: This store has 3 or more consecutive shifts with below-average sales.")
            
            # Create a summary table for the flagged shifts
            dip_summary_table = consecutive_dips_df[['shift_opening_time', 'shift_slot', 'sales_difference', 'performance_pct']].sort_values(by='shift_opening_time')
            dip_summary_table.rename(columns={'sales_difference': 'Sales Difference (₱)'}, inplace=True)
            
            # Display the table of consecutive dips
            display(dip_summary_table.style.format({
                'shift_opening_time': '{:%a, %b %d - %I:%M %p}', 'Sales Difference (₱)': '{:+,.2f}', 'performance_pct': '{:+.2f}%'
            }).set_caption("Flagged Consecutive Dip Shifts"))
        else:
            print("\n✅ OK: No consecutive dip alerts for this store.")
