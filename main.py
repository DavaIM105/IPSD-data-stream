from google.cloud import bigquery
import yfinance as yf
import pandas as pd
import logging
import os

def etl_pipeline():
    try:
        # Setup logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')

        # Paths untuk file
        new_data = "data_saham.csv"
        trans_data = "[Trans]data_saham.csv"
        latest_data = "latest_data_saham.csv"

        # Step 1: Extract - Ambil data dari Yahoo Finance
        symbols = ["BBCA.JK", "BBRI.JK"]
        logging.info(f"Fetching data for symbols: {symbols}")
        data = yf.download(tickers=symbols, period="1d", interval="5m")
        data.to_csv(new_data)
        
        if data.empty:
            logging.warning("No data fetched. Terminating pipeline.")
            return

        # Step 2: Transform - Bersihkan dan validasi data
        raw_data = pd.read_csv(new_data, index_col=0, header=[0, 1])
        logging.info("Cleaning and preparing data for BigQuery...")
        
        
        # Bersihkan dan format data
        raw_data.fillna(0, inplace=True)  # Isi NaN dengan 0
        raw_data.index = pd.to_datetime(raw_data.index)  # Konversi indeks ke datetime
        
        # Ubah zona waktu ke UTC+7
        raw_data.index = raw_data.index.tz_convert('Asia/Jakarta')
        logging.info("Datetime index converted to UTC+7.")
        
        # Hanya ambil kolom "Close"
        raw_data = raw_data['Close']
        logging.info("Filtered data to include only 'Close' column.")
        
        # Format nama kolom agar sesuai BigQuery
        raw_data.columns = [col.replace('.', '-') for col in raw_data.columns]
        
        # Simpan data pertama kali
        raw_data.to_csv(trans_data)
        
        logging.info("Data cleaning completed.")
        logging.info(f"Transformed Data saved to {trans_data}")

        # # Step 3: Load - Muat data ke BigQuery
        # logging.info("Loading data to BigQuery...")
        # client = bigquery.Client()
        # table_id = "data-stream-spread.dataset_stream_saham.saham_bca_bri"

        # job_config = bigquery.LoadJobConfig(
        #     autodetect=True,
        #     write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        # )

        # # Muat DataFrame ke BigQuery
        # add_data = pd.read_csv(trans_data, index_col=0, parse_dates=True)
        # job = client.load_table_from_dataframe(add_data, table_id, job_config=job_config)
        # job.result()  # Tunggu hingga proses selesai
        # logging.info(f"Data successfully loaded to BigQuery table: {table_id}")

    except Exception as e:
        logging.error(f"An error occurred: {e}", exc_info=True)


# Jalankan pipeline
etl_pipeline()
