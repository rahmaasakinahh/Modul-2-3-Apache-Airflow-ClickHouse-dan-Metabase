# Task 0 : _Setup & Installation_

---

# Task 1 : _Merancang Apache Airflow DAG_

---

# Task 2 : _Mengelola Data di ClickHouse_
**(membuat database & tabel, mendefinisikan schema, serta memastikan data berhasil di-load dari pipeline ke ClickHouse)**

---

Membaca data mentah dari file Parquet hasil `fetch_orders.py`, lalu memrosesnya sebelum data masuk ke ClickHouse. Secara garis besar, script menjalankan tiga proses utama: flatten data bertingkat, membersihkan nilai yang tidak valid, dan menyimpan hasilnya ke file Parquet. Berikut tiap prosesnya:

### Setup & Konfigurasi

<img width="1510" height="672" alt="image" src="https://github.com/user-attachments/assets/e9678201-2cea-40fd-b616-59c5931fe4ae" />

Mendefinisikan path input/output dan mengaktifkan logging agar setiap proses terekam di Airflow task logs.

### Inisialisasi Fungsi & Membaca Data

<img width="1588" height="710" alt="image" src="https://github.com/user-attachments/assets/8f794f2b-dc45-4a7f-a0af-3e261a4a8797" />

Membaca file Parquet dari Data Lake, mengubahnya menjadi format yang bisa diproses baris per baris, dan menyiapkan dua wadah kosong untuk menampung hasil flatten nantinya.

---

Selanjutnya adalah menjalankan tiga proses utama: 
1. flatten data bertingkat
2. membersihkan nilai yang tidak valid
3. menyimpan hasilnya ke file Parquet

---

### 1. Flattening Nested Data

<img width="1788" height="1166" alt="image" src="https://github.com/user-attachments/assets/41e2dd85-ac54-4aac-9717-bf066d65ac83" />

Data dari API bentuknya bertingkat, jadi 1 order bisa punya banyak produk di dalamnya. Karena ClickHouse tidak bisa menyimpan data bertingkat, script ini memecahnya menjadi 2 tabel terpisah yang dihubungkan lewat `order_id`.

Pemisahan ini sesuai **normalisasi database**, yaitu untuk menghindari duplikasi informasi order di setiap baris produk.

|Tabel|Isi|Contoh kolom|
|---|---|---|
|`orders`|Informasi level order|`order_id`, `user_id`, `order_dow`, `order_hour_of_day`|
|`order_items`|Detail produk per order|`product_name`, `aisle`, `department`, `reordered`|

### 2. Handling Missing Values

<img width="1588" height="406" alt="image" src="https://github.com/user-attachments/assets/4de94641-d39c-44b4-9d5d-4b1d2479790f" />

Saat inspeksi data, ditemukan nilai `"missing"` (berupa teks) pada kolom `aisle` dan `department` untuk beberapa produk. Nilai ini diganti menjadi `NULL` agar data tersimpan dengan benar di ClickHouse.

### 3. Output

<img width="1512" height="482" alt="image" src="https://github.com/user-attachments/assets/4716beff-a0ef-494f-a5e7-4a5c8a27c008" />

Hasil transform disimpan sebagai dua file Parquet terpisah, dilanjut dengan di-load ke ClickHouse di next step.

|File|Isi|
|---|---|
|`orders.parquet`|Data level order yang sudah di-flatten|
|`order_items.parquet`|Data detail produk per order|

---

### Logging & Error Handling

<img width="2350" height="596" alt="image" src="https://github.com/user-attachments/assets/283f9772-05e8-4de9-97a8-a4a0c242cafc" />

Kalau semua proses berhasil, script mencetak pesan sukses beserta jumlah data yang berhasil diproses. Kalau ada yang error di tengah jalan, pesan errornya langsung dicatat dan pipeline berhenti.

# Task 3 : _Membuat Visualisasi & Questions di Metabase_

---

# Task 4 : _Membangun Dashboard di Metabase_

---
