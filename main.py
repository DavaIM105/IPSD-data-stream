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
from ftplib import FTP
from google.cloud import bigquery

def etl_pipeline(request):
    ftp_host, ftp_port = '127.0.0.1', 21  # Alamat server FTP
    ftp_user = 'etl_user'    # Username FTP
    ftp_pass = 'pass'    # Password FTP
    data_file = 'data_saham.csv'  # Nama file yang ingin diunduh di server
    local_file = 'downloaded_data_saham.csv'  # Nama file lokal setelah diunduh
    ftp_dir = "./ftp"
    
    # Koneksi ke server FTP
    try:
        ftp = FTP(timeout=30)
        
        ftp.connect(ftp_host, ftp_port)
        ftp.login(user=ftp_user, passwd=ftp_pass)
        
        ftp.set_pasv(True)  # Gunakan mode pasif
        ftp.cwd(ftp_dir)
        
        # Extract: Unduh file dari FTP
        with open(local_file, 'wb') as f:
            ftp.retrbinary(f'RETR {data_file}', f.write)
        print(f"File '{ data_file}' berhasil diunduh sebagai '{local_file}'")
        ftp.quit()
        
        # Transform: Bersihkan data
        df = pd.read_csv(local_file)
        df_cleaned = df.drop_duplicates()
        df_cleaned.to_csv('/cleaned_data.csv', index=False)
        
        # Load: Muat ke BigQuery
        client = bigquery.Client()
        table_id = "data-stream-spread.dataset_stream_saham.transformed_stream_saham"
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=3,
            autodetect=True
        )
        with open('/cleaned_data.csv', 'rb') as source_file:
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()
        
        return "ETL Pipeline executed successfully"
        

    except Exception as e:
        print(f"Proses ETL Gagal: {e}")
        
    
    # ftp = FTP('ftp.example.com')
    # ftp.login(user='username', passwd='password')
    # ftp.cwd('/path/to/data')
    # with open('/tmp/data.csv', 'wb') as file:
    #     ftp.retrbinary('RETR data.csv', file.write)
    # ftp.quit()


    