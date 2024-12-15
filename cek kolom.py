import pandas as pd

# Membaca file CSV

df = pd.read_csv('[Trans]data_saham.csv', sep=';')

# Periksa beberapa baris pertama
print(df.head())
print(df.info())  # Cek informasi kolom
