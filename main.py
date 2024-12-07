import pandas as pd
import yfinance as yf
import os
from ftplib import FTP
from google.cloud import bigquery


def etl_pipeline():
    try:
        # Extract: Ambil data Yahoo Finance
        symbols = ["BBCA.JK", "TLKM.JK", "UNVR.JK"]
        data = yf.download(tickers=symbols, period="5d", interval="1m")
        data.to_csv("latest_data_saham.csv")
    
        
        # Transform: Bersihkan data
        old_data_name = "data_saham.csv"
        latest_data_name = "latest_data_saham.csv"
        if os.path.exists(old_data_name):
            old_data = pd.read_csv(old_data_name, index_col=0, header=[0,1])
            data.to_csv(old_data_name)
            new_data = data[~data.index.isin(old_data.index.astype(data.index.dtype))]
            
        new_data.to_csv(latest_data_name)
            
        print(f"Data berhasil disimpan ke {latest_data_name}")
            
        df = pd.read_csv(latest_data_name, index_col=0, header=[0,1])
        df.fillna(value=0, inplace=True)
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        
        # Flatten multi-index kolom
        df.columns = ['_'.join(map(str, col)) for col in df.columns]
        # Ganti karakter yang tidak valid pada nama kolom
        df.columns = [col.replace('.', '-') for col in df.columns]
        df.index = pd.to_datetime(df.index)
        
        df.to_csv(latest_data_name)
    
        
        # Load: Muat ke BigQuery
        client = bigquery.Client()
        table_id = "data-stream-spread.dataset_stream_saham.transformed_stream_saham"
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND

        )
        
        # Menulis DataFrame ke BigQuery
        print("Loading data ke BigQuery...")
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print("Data berhasil diupload ke BigQuery.")
    except Exception as e:
        print("Error:", e)
    
    
etl_pipeline()    