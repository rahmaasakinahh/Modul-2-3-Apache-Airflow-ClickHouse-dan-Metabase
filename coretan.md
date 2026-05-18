---

# **Orders Analytics Pipeline**

---

> Seleksi Calon Admin MCI 2026 — Modul 2 & 3 | Kelompok 12 Pipeline Orchestration & Data Visualization

|Anggota Kelompok|NRP|
|---|---|
|Nasyita Larashati Ertyananda|5025241201|
|Rahma Sakinah|5025241222|

---

# **Architecture**

---

terdiri dari empat komponen utama yang terhubung secara berurutan:

```
[Orders API]
     │
     ▼
[Apache Airflow]
     │
     ▼
fetch_orders → transform_orders → load_to_clickhouse
                                        │
                                        ▼
                                  [ClickHouse]
                                  ├── mci_db.orders
                                  └── mci_db.order_items
                                        │
                                        ▼
                                   [Metabase]
                              Orders Analytics Dashboard
```

Data diambil dari Orders API, lalu diproses secara berurutan oleh tiga task di Airflow (fetch, transform, dan load). Setelah data masuk ke ClickHouse, Metabase membacanya dan menampilkan hasilnya dalam bentuk dashboard.

---

---

# Setup & Installation

---

`requirements.txt`

<img width="664" height="482" alt="image" src="https://github.com/user-attachments/assets/e2f5c9ee-9adb-4cfb-85ed-2db0ff3c5545" />

Isinya seluruh library Python yang dibutuhkan pipeline. Requirements yang digunakan sama dengan yang dipakai pada sesi hands-on.

|Library|Versi|Fungsi|
|---|---|---|
|`pyspark`|3.5.1|Framework big data processing|
|`clickhouse-driver`|0.2.7|Koneksi Python ke ClickHouse|
|`pandas`|2.2.1|Manipulasi dan transformasi data|
|`requests`|2.31.0|Mengambil data dari API|
|`pyarrow`|15.0.2|Membaca dan menulis file Parquet|

`Dockerfile`

<img width="1094" height="710" alt="image" src="https://github.com/user-attachments/assets/e3ed6568-653d-418c-b82c-420fca876328" />

digunakan untuk menyiapkan sebelum kerja Airflow, jadi memastikan semua library dan tools yang dibutuhkan pipeline sudah terinstall di dalamnya.

|Bagian|Keterangan|
|---|---|
|`FROM apache/airflow:2.9.1-python3.11`|Base image Airflow versi 2.9.1 dengan Python 3.11|
|`RUN apt-get install default-jre-headless`|Install Java untuk menjalankan Apache Spark|
|`COPY requirements.txt`|Menyalin file requirements ke dalam container|
|`RUN pip install -r requirements.txt`|Install semua library Python yang dibutuhkan|

`docker-compose.yml`

<img width="2372" height="2724" alt="image" src="https://github.com/user-attachments/assets/a93a7ba9-bd61-4ddc-a429-3b214de12eb9" />

menjalankan semua service yang dibutuhkan pipeline dalam satu perintah. Terdapat 6 service yang berjalan bersamaan:

|Service|Port|Fungsi|
|---|---|---|
|`postgres`| - |Database internal Airflow untuk menyimpan metadata|
|`airflow-init`| - |Inisialisasi database dan membuat akun admin Airflow|
|`airflow-webserver`|8080|Tampilan web Airflow untuk memantau pipeline|
|`airflow-scheduler`| - |Menjalankan DAG sesuai jadwal yang ditentukan|
|`clickhouse-server`|8123, 9000|Database warehouse tempat data order disimpan|
|`metabase`|3000|Tampilan web untuk visualisasi dan dashboard|

Semua service Airflow menggunakan konfigurasi yang sama (x-airflow-common), jadi folder dags/ dan data_lake/ di komputer lokal terhubung langsung ke dalam container sehingga perubahan file langsung terbaca tanpa di restart.

---

# Task 1 : _Merancang Apache Airflow DAG_

---

hafipwebguorbngaklrnbguebobgrgboueanourhgakslnioowbrvwirpbgh

<img width="1918" height="1026" alt="image" src="https://github.com/user-attachments/assets/879ef40a-0c3a-4f69-b7bd-a361149109d7" />

<img width="1918" height="1026" alt="image" src="https://github.com/user-attachments/assets/0be2025b-f30c-49fa-9af9-ab5d002d3b53" />

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

---

# Task 3 : _Memuat Data ke ClickHouse_

---

Membaca dua file Parquet hasil transform, lalu memuatnya ke ClickHouse. Secara garis besar, script menjalankan empat proses utama: koneksi ke ClickHouse, membuat database & tabel, membersihkan data lama, dan insert data baru. Berikut tiap prosesnya:

### Setup & Konfigurasi

<img width="1388" height="634" alt="image" src="https://github.com/user-attachments/assets/adbe104d-d2d1-4f21-87e2-f794ec66d1af" />

Mendefinisikan path input dari dua file Parquet dan mengaktifkan logging agar setiap proses tercatat di Airflow task logs.

### Inisialisasi Fungsi & Membaca Data

<img width="1186" height="748" alt="image" src="https://github.com/user-attachments/assets/ae6fdc77-8240-4d7c-87b3-e6b9f293f3c8" />

Membaca dua file Parquet hasil transform yaitu `orders.parquet` dan `order_items.parquet` lalu membuka koneksi ke ClickHouse.

---

Selanjutnya adalah menjalankan empat proses utama:
1. Membuat database & tabel
2. Membersihkan data lama (TRUNCATE)
3. Insert data baru
4. Logging & Error Handling

---

### 1. Membuat Database & Tabel

<img width="1140" height="1394" alt="image" src="https://github.com/user-attachments/assets/d26be84a-a33c-4a2e-8f82-3566509c194c" />

Membuat database `mci_db` dan dua tabel jika belum ada. Menggunakan `CREATE TABLE IF NOT EXISTS` agar aman dijalankan berkali-kali tanpa error, tabel tidak akan dibuat ulang jika sudah ada.

|Tabel|Engine|Order By|
|---|---|---|
|`mci_db.orders`|MergeTree|`order_id`|
|`mci_db.order_items`|MergeTree|`(order_id, product_id)`|

### 2. Membersihkan Data Lama (TRUNCATE)

<img width="1080" height="368" alt="image" src="https://github.com/user-attachments/assets/41bc4a99-b7fb-4ff6-8bb6-52a0ac42a4b2" />

Sebelum data baru dimasukkan, data lama di tabel dihapus dulu menggunakan `TRUNCATE`. Jadi setiap kali pipeline jalan, tabel bersih dan isinya hanya data terbaru serta tidak ada data yang dobel.

### 3. Insert Data

<img width="1420" height="634" alt="image" src="https://github.com/user-attachments/assets/01fc6f3d-2680-4adf-a830-0a5bc8b33fc3" />

Data dari file Parquet diubah ke format yang bisa dibaca ClickHouse, lalu dimasukkan ke tabel. Proses insert hanya berjalan kalau datanya memang ada, jika kalau kosong langsung dilewati.

---

### Logging & Error Handling

<img width="1504" height="596" alt="image" src="https://github.com/user-attachments/assets/32597fa0-3b44-40db-9779-07d9c3d8f6bf" />

Kalau semua proses berhasil, script mencetak pesan sukses beserta jumlah data yang berhasil di-insert. Kalau ada yang error di tengah jalan, pesan errornya langsung dicatat dan pipeline berhenti.

---

# Task 4 : _Membuat Visualisasi & Questions di Metabase_

---

hafipwebguorbngaklrnbguebobgrgboueanourhgakslnioowbrvwirpbgh

