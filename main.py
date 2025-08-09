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

# --- Configuration ---
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BASELINE_DAYS = 90
ANALYSIS_DAYS = 7
client = bigquery.Client(project=PROJECT_ID)
store_name_map = {
    '0c2650c7-6891-445f-bfb6-ad9a996512de': 'CHH SB Co', '82034dd5-a404-43b4-9b2d-674e3bab0242': 'S4',
    '9d4f5c35-b777-4382-975f-33d72a1ac476': 'SB Co Ebloc3', 'a3291a95-e8e7-45c6-9d44-df81b36a1d37': 'Dtan',
    'b0baf2e5-2b71-41b6-a5f3-b280c43dc4e3': 'UC Med SB', 'c9015f99-b75b-4ee9-ac52-d76eb16cab37': 'Sugbo Sentro',
    '1153f4fd-c8a7-495f-8a9f-2c6c8aa2cc6e': 'Sky1 SB', '1a138ff9-d87f-4e82-951d-a0f89dec364d': 'Wipro',
    '20c5e48d-dc7c-4ad0-8f3f-f833338e5284': 'Alpha Simply', 'b9b66c09-dbfe-4ea4-9489-05b3320ddce2': 'Alliance Simply',
    '61928236-425a-40c8-9b21-38eafd0115f1': 'Trial Store 2', 'f4bf31ef-12a8-4758-b941-8b8be11d7c23': 'Skyrise 3',
    'a254eb83-5e8b-4ff6-86f9-a26d47114288': 'JY SB'
}

# --- Helper Function: Your Sales Dip Script ---
def run_sales_dip_analysis():
    print("Running sales dip analysis...")
    # (Paste your entire "Sales dip tracker" script logic here)
    # ...
    # IMPORTANT: At the end, instead of dp.display(), return the HTML and the chart
    # Example of what the end of the function should look like:
    if not anomalies.empty:
        # (your timezone conversion and formatter setup)
        html_output = anomalies[display_cols].style.format(formatter=formatter, na_rep='Still Open').to_html()
        
        # --- Visualization ---
        # ... (Your sns.FacetGrid code) ...
        img_buffer = io.BytesIO()
        plt.savefig(img_buffer, format='png', bbox_inches='tight')
        img_buffer.seek(0)
        chart_image_data = img_buffer.read()
        plt.close()
        
        return html_output, chart_image_data
    else:
        return "<p>No significant sales dips found in the last 7 days.</p>", None


# --- Helper Function: Your Ratio Script ---
def run_ratio_analysis():
    print("Running Americano vs. Spanish Latte ratio analysis...")
    # (Paste your entire "Americano VS Spanish Latte" script logic here)
    # ...
    # IMPORTANT: At the end, instead of dp.display(), return the HTML
    # Example of what the end of the function should look like:
    if not anomalies.empty:
        # (your column renaming and formatter setup)
        html_output = anomalies[display_cols].style.format(formatter).to_html()
        return html_output
    else:
        return "<p>No unusual Americano vs. Spanish Latte ratios found.</p>"


# --- Main Function to be triggered ---
def generate_and_email_report():
    """
    This is the main entry point. It runs both analyses and sends a single email report.
    """
    print("🚀 Starting anomaly detection report generation...")

    # --- Run Both Analyses ---
    sales_dip_html, sales_dip_chart = run_sales_dip_analysis()
    ratio_anomaly_html = run_ratio_analysis()

    # --- Prepare and Send Email ---
    sender_email = os.environ.get("SENDER_EMAIL")
    sender_password = os.environ.get("SENDER_PASSWORD")
    recipient_email = os.environ.get("RECIPIENT_EMAIL")

    if not all([sender_email, sender_password, recipient_email]):
        print("🛑 Missing email configuration. Cannot send report.")
        return

    subject = f"Weekly Sales Anomaly Report - {pd.Timestamp.now(tz='Asia/Manila').strftime('%Y-%m-%d')}"
    
    body_html = f"""
    <html>
    <head>
        <style>
            body {{ font-family: sans-serif; color: #333; }}
            table {{ border-collapse: collapse; margin: 25px 0; font-size: 0.9em; min-width: 400px; box-shadow: 0 0 20px rgba(0, 0, 0, 0.15); }}
            th, td {{ padding: 12px 15px; border: 1px solid #dddddd; }}
            thead tr {{ background-color: #009879; color: #ffffff; text-align: left; }}
            tbody tr {{ border-bottom: 1px solid #dddddd; }}
            tbody tr:nth-of-type(even) {{ background-color: #f3f3f3; }}
            h2 {{ color: #005f4e; border-bottom: 2px solid #009879; padding-bottom: 5px; }}
        </style>
    </head>
    <body>
        <h2>🚨 Shifts with Unusually Low Sales</h2>
        {sales_dip_html}
        <br>
        <h2>📈 Shifts with High Americano vs. Spanish Latte Ratio</h2>
        {ratio_anomaly_html}
        <br>
        <p>See attached chart for sales distribution.</p>
    </body>
    </html>
    """
    
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = recipient_email
    message['Subject'] = subject
    message.attach(MIMEText(body_html, 'html'))
    
    if sales_dip_chart:
        image = MIMEImage(sales_dip_chart, name="sales_distribution.png")
        message.attach(image)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
        print("✅ Email report sent successfully.")
    except Exception as e:
        print(f"🔥 Failed to send email: {e}")

# --- Script entry point ---
if __name__ == "__main__":
    generate_and_email_report()
