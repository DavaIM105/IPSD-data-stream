# import yfinance as yf
# from datetime import datetime

# def fetch_and_save_data():
#     symbols = ["BBCA.JK", "TLKM.JK", "UNVR.JK"]  # Simbol saham Indonesia
#     data = yf.download(tickers=symbols, interval="1m")  # Data real-time (1 menit)

#     # Nama file CSV berdasarkan waktu
#     # file_name = f"data_saham_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
#     file_name = f"data_saham.csv"
#     data.to_csv(file_name)
#     print(f"[{datetime.now()}] Data berhasil disimpan ke {file_name}")
    
import pandas as pd
import yfinance as yf
import os
from ftplib import FTP
from google.cloud import bigquery



def etl_pipeline():
    # ftp_host, ftp_port = '127.0.0.1', 21  # Alamat server FTP
    # ftp_user = 'etl_user'    # Username FTP
    # ftp_pass = 'pass'    # Password FTP
    # data_file = 'data_saham.csv'  # Nama file yang ingin diunduh di server
    # local_file = 'downloaded_data_saham.csv'  # Nama file lokal setelah diunduh
    # ftp_dir = "./ftp"
    
    # # Koneksi ke server FTP
    # try:
    #     ftp = FTP(timeout=30)
        
    #     ftp.connect(ftp_host, ftp_port)
    #     ftp.login(user=ftp_user, passwd=ftp_pass)
        
    #     ftp.set_pasv(True)  # Gunakan mode pasif
    #     ftp.cwd(ftp_dir)
        
    # # Extract: Unduh file dari FTP
    # with open(local_file, 'wb') as f:
    #     ftp.retrbinary(f'RETR {data_file}', f.write)
    # print(f"File '{ data_file}' berhasil diunduh sebagai '{local_file}'")
    # ftp.quit()
    # fetch_and_save_data()
    
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
        new_data = data[~data.index.isin(old_data.index)]
        
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
    print("Menghubungkan ke BigQuery...")
    client = bigquery.Client()
    table_id = "data-stream-spread.dataset_stream_saham.transformed_stream_saham"
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND

    )
    
    # Menulis DataFrame ke BigQuery
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    print("Loading data...")
    job.result()
    print("Data berhasil diupload ke BigQuery.")

    
    
etl_pipeline()
    # except Exception as e:
    #     print(f"Proses ETL Gagal: {e}")
        
    
    # ftp = FTP('ftp.example.com')
    # ftp.login(user='username', passwd='password')
    # ftp.cwd('/path/to/data')
    # with open('/tmp/data.csv', 'wb') as file:
    #     ftp.retrbinary('RETR data.csv', file.write)
    # ftp.quit()


    