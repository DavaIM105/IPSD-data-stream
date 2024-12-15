import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

# Membaca file CSV
# file_path = "/mnt/data/[Trans]data_saham.csv"
# df = pd.read_csv(file_path)
df = pd.read_csv('[Trans]data_saham.csv', sep=';')

# Ganti nama kolom ini dengan kolom di file CSV Anda
tanggal_kolom = 'Datetime'    # Nama kolom tanggal
nilai_kolom1 = 'BBCA-JK'      # Nama kolom data pertama
nilai_kolom2 = 'BBRI-JK'      # Nama kolom data kedua

# Konversi kolom tanggal ke tipe datetime
df[tanggal_kolom] = pd.to_datetime(df[tanggal_kolom], errors='coerce')

# Hapus baris dengan nilai tanggal yang tidak valid
df = df.dropna(subset=[tanggal_kolom])

# Urutkan data berdasarkan tanggal
df.sort_values(by=tanggal_kolom, inplace=True)

# Membuat subplots
fig, axes = plt.subplots(2, 1, figsize=(12, 10), sharex=True)  # Dua grafik vertikal

# Grafik 1
axes[0].plot(df[tanggal_kolom], df[nilai_kolom1], marker='o', color='blue', label=f'{nilai_kolom1}')
axes[0].set_title('BBCA-JK', fontsize=14)
axes[0].set_ylabel('Nilai Kolom 1', fontsize=12)
axes[0].grid(True)
axes[0].legend()

# Grafik 2
axes[1].plot(df[tanggal_kolom], df[nilai_kolom2], marker='x', color='red', label=f'{nilai_kolom2}')
axes[1].set_title('BBRI-JK', fontsize=14)
axes[1].set_ylabel('Nilai Kolom 2', fontsize=12)
axes[1].set_xlabel('Tanggal', fontsize=12)
axes[1].grid(True)
axes[1].legend()

# Sesuaikan layout dan simpan file gambar
plt.tight_layout()
timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
output_file = f"grafik_streaming_{timestamp}.png"
plt.savefig(output_file)
plt.show()

print(f"Gambar telah disimpan di: {output_file}")
