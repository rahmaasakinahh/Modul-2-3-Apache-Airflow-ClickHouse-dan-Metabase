# 🚀 Orders Analytics Pipeline

![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black)
![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=for-the-badge&logo=metabase&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)

> **MCI2026 — Task 2 | Kelompok 12**
> Pipeline Orchestration & Data Visualization

---

## 📌 Overview

Pipeline ini mengotomasi alur data end-to-end dari **Orders API** hingga **dashboard interaktif**. Data order diambil secara otomatis menggunakan Apache Airflow, ditransformasi dan dibersihkan, lalu dimuat ke ClickHouse sebagai data warehouse, dan divisualisasikan melalui Metabase untuk menghasilkan insight bisnis yang actionable.

---

## 🏗️ Architecture

```
[Orders API]
     │
     ▼
[Apache Airflow]
     │
     ├── fetch_orders      → Ambil data dari API
     ├── transform_orders  → Flatten nested data + handle missing values
     └── load_to_clickhouse → Load ke ClickHouse
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

---

## 🛠️ Tech Stack

| Tool | Versi | Fungsi |
|---|---|---|
| Apache Airflow | 2.9.1 | Pipeline orchestration & scheduling |
| ClickHouse | Latest | Columnar data warehouse |
| Metabase | Latest | Visualisasi & business dashboard |
| Docker | - | Containerization semua service |
| Python | 3.11 | Scripting pipeline |
| Pandas | 2.2.1 | Data manipulation |
| clickhouse-driver | 0.2.7 | Koneksi Python → ClickHouse |

---

## 📁 Repository Structure

```
MCI2026_Task2_Kelompok12/
├── dags/
│   ├── orders_pipeline.py        # DAG utama Airflow
│   └── scripts/
│       ├── fetch_orders.py       # Task 1: Fetch dari API
│       ├── transform_orders.py   # Task 2: Transform & flatten
│       └── load_orders.py        # Task 3: Load ke ClickHouse
├── sql/
│   ├── ddl.sql                   # DDL pembuatan tabel ClickHouse
│   └── metabase_queries.sql      # Query visualisasi Metabase
├── docker-compose.yml            # Setup semua service
├── Dockerfile                    # Custom Airflow image
├── requirements.txt              # Python dependencies
└── README.md
```

---

## ⚙️ Setup & Installation

### Prerequisites
- Docker Desktop (running)
- Git

### Langkah-langkah

**1. Clone repository**
```bash
git clone https://github.com/<username>/MCI2026_Task2_Kelompok12.git
cd MCI2026_Task2_Kelompok12
```

**2. Jalankan semua service**
```bash
docker-compose up -d
```

**3. Inisialisasi Airflow**
```bash
docker-compose run --rm airflow-init
docker-compose restart airflow-webserver
```

**4. Akses service**
| Service | URL | Kredensial |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | Setup saat pertama buka |
| ClickHouse | http://localhost:8123 | admin / rahasia |

---

## 🔄 Pipeline Explanation

### DAG: `orders_pipeline`
- **Schedule**: `@daily` — berjalan otomatis setiap hari
- **Max Active Runs**: 1 — mencegah race condition
- **Retries**: 1x dengan delay 5 menit

### Task 1: `fetch_orders`
Mengambil data dari API `http://96.9.212.102:8000/orders` menggunakan `requests`, lalu menyimpannya sebagai file Parquet ke Data Lake lokal. Menggunakan `logging` (bukan `print`) agar output terekam proper di Airflow task logs.

### Task 2: `transform_orders`
Melakukan **flattening nested data** — karena setiap order memiliki banyak produk (nested array), data dipecah menjadi dua struktur flat:
- `orders` → satu baris per order
- `order_items` → satu baris per produk per order

Keduanya disimpan sebagai file Parquet terpisah, siap untuk di-load ke ClickHouse.

### Task 3: `load_to_clickhouse`
Memuat data dari Parquet ke ClickHouse. Pipeline ini dirancang **idempotent** — setiap eksekusi diawali dengan `TRUNCATE TABLE` sebelum `INSERT`, sehingga aman dijalankan berkali-kali tanpa duplikasi data.

---

## 🗄️ Database Schema

Data dari API berbentuk **nested** (1 order memiliki banyak produk), sehingga didesain menjadi **2 tabel terpisah** yang dihubungkan via `order_id`.

### `mci_db.orders`
| Kolom | Tipe | Keterangan |
|---|---|---|
| order_id | Int32 | Primary key |
| user_id | Int32 | ID user |
| order_number | Int32 | Urutan order ke-berapa |
| order_dow | Int8 | Hari dalam seminggu (0=Sunday) |
| order_hour_of_day | Int8 | Jam order (0-23) |
| days_since_prior_order | Nullable(Float32) | Jarak hari dari order sebelumnya |
| eval_set | String | Kategori dataset |

### `mci_db.order_items`
| Kolom | Tipe | Keterangan |
|---|---|---|
| order_id | Int32 | Foreign key ke orders |
| product_id | Int32 | ID produk |
| product_name | String | Nama produk |
| aisle_id | Int32 | ID lorong |
| aisle | Nullable(String) | Nama lorong |
| department_id | Int32 | ID departemen |
| department | Nullable(String) | Nama departemen |
| add_to_cart_order | Int32 | Urutan ditambah ke keranjang |
| reordered | Int8 | Pernah dipesan sebelumnya (0/1) |

> **Keputusan desain**: `days_since_prior_order` menggunakan `Nullable(Float32)` karena order pertama seorang user selalu bernilai `null` — ini valid secara logika bisnis, bukan data kotor.

---

## 🔍 Data Quality

Sebelum di-load ke ClickHouse, dilakukan inspeksi dan penanganan kualitas data:

| Temuan | Kolom | Penanganan |
|---|---|---|
| Nilai `"missing"` (string) | `aisle`, `department` | Di-replace menjadi `NULL` di tahap transform |
| Nilai `null` | `days_since_prior_order` | Dibiarkan `NULL` — valid untuk order pertama user |

---

## 📊 Dashboard & Insights

Dashboard **Orders Analytics Dashboard** di Metabase terdiri dari 6 visualisasi:

### 📅 Order Behavior

**Orders by Day of Week**
> Monday memiliki jumlah order tertinggi (27 orders), menunjukkan pola belanja yang dominan di awal minggu. Sunday memiliki jumlah order terendah.

*(screenshot)*

**Orders by Hour of Day**
> Order paling banyak terjadi pada jam 11 siang (12 orders), mengindikasikan waktu peak belanja di siang hari.

*(screenshot)*

### 🛍️ Product Analysis

**Top 10 Most Ordered Products**
> Banana menjadi produk paling banyak dipesan, diikuti produk-produk organic. Ini mengindikasikan preferensi konsumen terhadap produk segar dan healthy.

*(screenshot)*

**Top 10 Products by Reorder Rate**
> Banyak produk mencapai reorder rate 100%, menunjukkan loyalitas tinggi konsumen terhadap produk tertentu.

*(screenshot)*

### 🏪 Department & Patterns

**Top 5 Departments by Orders**
> Produce mendominasi dengan 40% dari total order, diikuti dairy eggs (22.7%). Ini menunjukkan mayoritas pembelian adalah produk segar dan perishable.

*(screenshot)*

**Average Items per Order by Day**
> Saturday dan Friday memiliki rata-rata item per order tertinggi, mengindikasikan konsumen cenderung belanja lebih banyak item menjelang akhir pekan.

*(screenshot)*

---

## ▶️ How to Run Pipeline

1. Buka browser → `http://localhost:8080`
2. Login dengan `admin / admin`
3. Aktifkan toggle DAG `orders_pipeline`
4. Klik tombol **▶ (Trigger DAG)**
5. Pantau progress — semua task harus berwarna **hijau**
6. Buka Metabase → `http://localhost:3000` untuk melihat dashboard

---

## 👥 Tim Kelompok 12

| Nama | Kontribusi |
|---|---|
| [Nama 1] | DAG Airflow, ClickHouse Schema, Data Transform |
| [Nama 2] | Metabase Dashboard, Visualisasi, Insight |
