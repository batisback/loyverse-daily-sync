import pandas as pd
from google.cloud import bigquery
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import os
import io

# For sending email
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

# --- 1. GLOBAL CONFIGURATION ---
# These settings are read from GitHub secrets
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# These settings are for the analysis logic
BASELINE_DAYS = 90
ANALYSIS_DAYS = 7

# Initialize the BigQuery client once
client = bigquery.Client(project=PROJECT_ID)

# Store name mapping for all analyses
store_name_map = {
    '0c2650c7-6891-445f-bfb6-ad9a996512de': 'CHH SB Co', '82034dd5-a404-43b4-9b2d-674e3bab0242': 'S4',
    '9d4f5c35-b777-4382-975f-33d72a1ac476': 'SB Co Ebloc3', 'a3291a95-e8e7-45c6-9d44-df81b36a1d37': 'Dtan',
    'b0baf2e5-2b71-41b6-a5f3-b280c43dc4e3': 'UC Med SB', 'c9015f99-b75b-4ee9-ac52-d76eb16cab37': 'Sugbo Sentro',
    '1153f4fd-c8a7-495f-8a9f-2c6c8aa2cc6e': 'Sky1 SB', '1a138ff9-d87f-4e82-951d-a0f89dec364d': 'Wipro',
    '20c5e48d-dc7c-4ad0-8f3f-f833338e5284': 'Alpha Simply', 'b9b66c09-dbfe-4ea4-9489-05b3320ddce2': 'Alliance Simply',
    '61928236-425a-40c8-9b21-38eafd0115f1': 'Trial Store 2', 'f4bf31ef-12a8-4758-b941-8b8be11d7c23': 'Skyrise 3',
    'a254eb83-5e8b-4ff6-86f9-a26d47114288': 'JY SB'
}

# --- 2. HELPER FUNCTIONS ---

def run_sales_dip_analysis():
    """Runs the sales dip analysis and returns an HTML table and a chart image."""
    print("Running sales dip analysis...")
    
    # --- BigQuery Query ---
    shift_sales_query = f"""
    WITH ShiftSales AS (
        SELECT
            s.id AS shift_number, s.opened_at, s.closed_at, s.store_id,
            COALESCE((SELECT SUM(CAST(JSON_EXTRACT_SCALAR(p, '$.money_amount') AS FLOAT64)) FROM UNNEST(s.payments) AS p), 0) AS total_sales
        FROM `{PROJECT_ID}.loyverse_data.new_final_shifts` AS s
        WHERE s.opened_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {BASELINE_DAYS} DAY)
    )
    SELECT
        shift_number, opened_at AS shift_opening_time, closed_at AS shift_closing_time, store_id, total_sales,
        EXTRACT(DAYOFWEEK FROM opened_at AT TIME ZONE 'Asia/Manila') AS day_of_week,
        EXTRACT(HOUR FROM opened_at AT TIME ZONE 'Asia/Manila') AS hour_of_day
    FROM ShiftSales ORDER BY store_id, shift_opening_time
    """
    
    # --- Data Fetching and Processing ---
    shift_data = client.query(shift_sales_query).to_dataframe()

    if shift_data.empty:
        return "<p>No sales dip data found to analyze.</p>", None
    
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

    # --- Anomaly Detection Logic ---
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

    if anomalies.empty:
        return f"<p>No suspicious sales dips found in the last {ANALYSIS_DAYS} days.</p>", None
        
    # --- Convert to HTML Table ---
    anomalies['shift_opening_time'] = anomalies['shift_opening_time'].dt.tz_convert('Asia/Manila')
    anomalies['shift_closing_time'] = pd.to_datetime(anomalies['shift_closing_time']).dt.tz_convert('Asia/Manila')
    display_cols = ['store_name', 'shift_opening_time', 'shift_closing_time', 'shift_slot', 'total_sales', 'avg_sales']
    formatter = {
        'total_sales': '{:,.2f}', 'avg_sales': '{:,.2f}', 
        'shift_opening_time': '{:%Y-%m-%d %H:%M}', 'shift_closing_time': '{:%Y-%m-%d %H:%M}'
    }
    html_output = anomalies[display_cols].style.format(formatter=formatter, na_rep='Still Open').to_html()

    # --- Generate Chart Image ---
    g = sns.FacetGrid(analysis_df, col="store_name", col_wrap=3, height=5, sharey=False, col_order=sorted(analysis_df['store_name'].unique()))
    g.map_dataframe(sns.boxplot, x='shift_slot', y='total_sales', showfliers=False, color='lightblue')
    g.map_dataframe(sns.stripplot, x='shift_slot', y='total_sales', jitter=True, alpha=0.5)
    g.map_dataframe(sns.stripplot, x='shift_slot', y='total_sales', data=anomalies, color='red', s=10, label='Anomalous Drop')
    g.set_titles("Store: {col_name}"); g.set_axis_labels("Shift Slot", "Total Sales")
    g.set_xticklabels(rotation=45); g.add_legend(); plt.tight_layout()
    
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', bbox_inches='tight')
    img_buffer.seek(0)
    chart_image_data = img_buffer.read()
    plt.close()

    return html_output, chart_image_data

def run_ratio_analysis():
    """Runs the Americano vs. Spanish Latte analysis and returns an HTML table."""
    print("Running Americano vs. Spanish Latte ratio analysis...")

    # --- BigQuery Query ---
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
    
    # --- Data Fetching and Processing ---
    data = client.query(query).to_dataframe()

    if data.empty:
        return "<p>No Americano or Spanish Latte data found to analyze.</p>"

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

    # --- Anomaly Detection Logic ---
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

    if anomalies.empty:
        return f"<p>No unusual Americano vs. Spanish Latte ratios found in the last {ANALYSIS_DAYS} days.</p>"

    # --- Convert to HTML Table ---
    anomalies.rename(columns={
        'shift_slot': 'Shift Type', 'avg_americano_orders': 'Avg Americano Orders',
        'Americano': 'Americano Orders', 'Spanish Latte': 'Spanish Latte Orders',
        'americano_vs_spanish_latte_pct': '% Americano vs Spanish Latte'
    }, inplace=True)
    display_cols = ['store_name', 'shift_opening_time', 'Shift Type', 'Avg Americano Orders', 'Americano Orders', 'Spanish Latte Orders', '% Americano vs Spanish Latte']
    formatter = {
        'shift_opening_time': '{:%Y-%m-%d %H:%M}', 'Avg Americano Orders': '{:.2f}',
        'Americano Orders': '{:.0f}', 'Spanish Latte Orders': '{:.0f}',
        '% Americano vs Spanish Latte': '{:.1%}'
    }
    html_output = anomalies[display_cols].style.format(formatter).to_html()
    return html_output

# --- 3. MAIN EMAIL FUNCTION ---
def generate_and_email_report():
    """Calls both analyses, builds, and sends a single email report."""
    print("🚀 Starting anomaly detection report generation...")

    # Run both analyses and get their HTML results
    sales_dip_html, sales_dip_chart = run_sales_dip_analysis()
    ratio_anomaly_html = run_ratio_analysis()

    # Get email credentials from GitHub secrets
    sender_email = os.environ.get("SENDER_EMAIL")
    sender_password = os.environ.get("SENDER_PASSWORD")
    recipient_email_str = os.environ.get("RECIPIENT_EMAIL") # Get the raw string

    if not all([sender_email, sender_password, recipient_email_str]):
        print("🛑 Missing email configuration secrets. Cannot send report.")
        return

    # --- FIX: Split the string of emails into a Python list ---
    # This handles "email1@x.com, email2@y.com" and removes any extra spaces.
    recipient_list = [email.strip() for email in recipient_email_str.split(',')]

    subject = f"Weekly Sales Anomaly Report - {pd.Timestamp.now(tz='Asia/Manila').strftime('%Y-%m-%d')}"
    
    # Construct the email body with some nice styling
    body_html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; color: #333; }}
            table {{ border-collapse: collapse; margin: 25px 0; font-size: 0.9em; min-width: 400px; box-shadow: 0 0 20px rgba(0, 0, 0, 0.15); }}
            th, td {{ padding: 12px 15px; border: 1px solid #dddddd; }}
            thead tr {{ background-color: #007bff; color: #ffffff; text-align: left; }}
            tbody tr {{ border-bottom: 1px solid #dddddd; }}
            tbody tr:nth-of-type(even) {{ background-color: #f3f3f3; }}
            h2 {{ color: #0056b3; border-bottom: 2px solid #007bff; padding-bottom: 5px; }}
        </style>
    </head>
    <body>
        <h2>🚨 Shifts with Unusually Low Sales</h2>
        {sales_dip_html}
        <br>
        <h2>📈 Shifts with High Americano vs. Spanish Latte Ratio</h2>
        {ratio_anomaly_html}
        <br>
        <p>See attached chart for store sales distribution.</p>
    </body>
    </html>
    """
    
    message = MIMEMultipart()
    message['From'] = sender_email
    # Use join to create a display-friendly string for the 'To' header
    message['To'] = ", ".join(recipient_list)
    message['Subject'] = subject
    message.attach(MIMEText(body_html, 'html'))
    
    # Attach the chart image if it exists
    if sales_dip_chart:
        image = MIMEImage(sales_dip_chart, name="sales_distribution.png")
        message.attach(image)

    # Send the email
    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            # Pass the recipient_list to the sendmail function
            server.sendmail(sender_email, recipient_list, message.as_string())
        print(f"✅ Email report sent successfully to: {', '.join(recipient_list)}")
    except Exception as e:
        print(f"🔥 Failed to send email: {e}")
