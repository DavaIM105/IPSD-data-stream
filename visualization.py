import pandas as pd
import matplotlib.pyplot as plt
from google.cloud import bigquery
from datetime import datetime
import logging
import os

# Mendapatkan timestamp, bulan, dan tahun saat ini
timestamp = datetime.now().strftime("%Y-%m-%d")
month = datetime.now().strftime("%B")
year = datetime.now().strftime("%Y")


try:
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] - %(message)s'
    )
    logging.info("Visualization started")

    # Setup Google Cloud Credentials
    cred_path = "service_account_key.json"
    if not os.path.exists(cred_path):
        logging.error(
            f"Google Cloud credentials file not found at {cred_path}")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

    # Setup BigQuery Client
    client = bigquery.Client()
    table_id = "data-stream-spread.dataset_stream_saham.saham_bca_bri"

    # Query untuk menarik data dari BigQuery
    logging.info("Fetching data from BigQuery...")
    query = f"""
    SELECT *
    FROM `{table_id}`
    WHERE Datetime > '{timestamp} 00:00:00'
    ORDER BY Datetime DESC
    """
    df = client.query(query).to_dataframe()
    print(df.head)
    # Pastikan Datetime terkonversi dengan benar
    df['Datetime'] = pd.to_datetime(df['Datetime'])
    df.set_index('Datetime', inplace=True)

    # Visualisasi data
    logging.info("Generating visualization...")
    fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)  # Dua grafik vertikal

    # grafik BBCA-JK
    
    axes[0].plot(df.index, df['BBCA-JK'], marker='o', color='blue', label='BBCA-JK')
    axes[0].set_title(f'Grafik Saham BBCA-JK dan BBRI-JK({timestamp})', fontsize=14)
    axes[0].set_ylabel('Harga', fontsize=12)
    axes[0].grid(True)
    axes[0].legend()
    
    # grafik BBRI-JK
    axes[1].plot(df.index, df['BBRI-JK'], marker='x', color='red', label='BBRI-JK')
    axes[1].set_ylabel('Harga', fontsize=12)
    axes[1].set_xlabel('Waktu(UTC)', fontsize=12)
    axes[1].grid(True)
    axes[1].legend()

    # Sesuaikan layout dan simpan file gambar
    plt.tight_layout()

    # Membuat path folder output
    output_dir = os.path.join(os.getcwd(), "visualisasi", year, month)

    # Membuat folder jika belum ada
    os.makedirs(output_dir, exist_ok=True)

    # Menyusun nama file output
    output_file = os.path.join(output_dir, f"grafik_streaming_{timestamp}.png")

    # Menyimpan gambar
    plt.savefig(output_file)

    logging.info("Visualization complete")

except Exception as e:
    logging.error(f"An error occurred during visualization: {e}", exc_info=True)
