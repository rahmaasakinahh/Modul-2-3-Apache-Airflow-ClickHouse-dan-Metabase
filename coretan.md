# Task 0 : _Setup & Installation_

---

# Task 1 : _Merancang Apache Airflow DAG_

---

# Task 2 : _Mengelola Data di ClickHouse_
**(membuat database & tabel, mendefinisikan schema, serta memastikan data berhasil di-load dari pipeline ke ClickHouse)**

---

Script ini membaca data mentah dari file Parquet hasil fetch_orders.py, lalu melakukan dua proses utama sebelum data masuk ke ClickHouse.
Data mentah dibaca dan disimpan dalam format Parquet, format kolomar yang lebih efisien dibanding JSON atau CSV karena menyimpan tipe data secara eksplisit dan lebih cepat dibaca saat di-load ke ClickHouse.

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

<img width="1512" height="368" alt="image" src="https://github.com/user-attachments/assets/5b858d4f-733e-48a8-8c8b-3cecffb13b8d" />

Hasil transform disimpan sebagai dua file Parquet terpisah, dilanjut dengan di-load ke ClickHouse di next step.

|File|Isi|
|---|---|
|`orders.parquet`|Data level order yang sudah di-flatten|
|`order_items.parquet`|Data detail produk per order|

# Task 3 : _Membuat Visualisasi & Questions di Metabase_

---

# Task 4 : _Membangun Dashboard di Metabase_

---
