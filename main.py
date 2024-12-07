from google.cloud import bigquery
import yfinance as yf
import pandas as pd
import logging
import os


def etl_pipeline():
    try:
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

        # Paths untuk file
        old_data_name = "data_saham.csv"
        latest_data_name = "latest_data_saham.csv"

        # Step 1: Extract - Ambil data dari Yahoo Finance
        symbols = ["BBCA.JK", "TLKM.JK", "UNVR.JK"]
        logging.info(f"Fetching data for symbols: {symbols}")
        data = yf.download(tickers=symbols, period="5d", interval="1m")

        if data.empty:
            logging.warning("No data fetched. Terminating pipeline.")
            return

        # Step 2: Transform - Bersihkan dan validasi data
        if os.path.exists(old_data_name):
            logging.info(f"Reading old data from {old_data_name}")
            old_data = pd.read_csv(old_data_name, index_col=0, header=[0, 1])
            new_data = data[~data.index.isin(old_data.index.astype(data.index.dtype))]
        else:
            logging.info("No previous data found. Starting fresh.")
            new_data = data

        # Simpan data terbaru
        new_data.to_csv(latest_data_name)
        data.to_csv(old_data_name)  # Simpan semua data sebagai referensi
        logging.info(f"Data saved to {latest_data_name} and {old_data_name}")

        # Bersihkan dan format data untuk BigQuery
        logging.info("Cleaning and preparing data for BigQuery...")
        df = pd.read_csv(latest_data_name, index_col=0, header=[0, 1])
        df.fillna(0, inplace=True)  # Isi NaN dengan 0
        df.drop_duplicates(inplace=True)  # Hapus duplikasi
        df.index = pd.to_datetime(df.index)  # Konversi indeks ke datetime
        print(pd.read_csv(old_data_name))

        # Gabungkan multi-index kolom
        df.columns = ['_'.join(map(str, col)) for col in df.columns]
        df.columns = [col.replace('.', '-') for col in df.columns]  # Format nama kolom agar sesuai BigQuery
        logging.info("Data cleaning completed.")
        print(pd.read_csv(latest_data_name))

        # Step 3: Load - Muat data ke BigQuery
        logging.info("Loading data to BigQuery...")
        client = bigquery.Client()
        table_id = "data-stream-spread.dataset_stream_saham.data_saham"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        # Muat DataFrame ke BigQuery
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Tunggu hingga proses selesai
        logging.info(f"Data successfully loaded to BigQuery table: {table_id}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)


# Jalankan pipeline
etl_pipeline()
