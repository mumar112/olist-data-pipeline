# Olist E-Commerce Data Engineering Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.11-orange?logo=dbt&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Containerised-2496ED?logo=docker&logoColor=white)
![GitHub Actions](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)
![Tableau](https://img.shields.io/badge/Dashboard-Tableau%20Public-E97627?logo=tableau&logoColor=white)

A production-style end-to-end data engineering pipeline built on real Brazilian e-commerce data. Raw CSV files are ingested, validated, transformed with dbt, tested automatically via CI/CD, containerised with Docker, and visualised in a Tableau dashboard.

---

## Table of Contents

- [Project Summary](#project-summary)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Dataset](#dataset)
- [Pipeline Phases](#pipeline-phases)
- [Project Structure](#project-structure)
- [Dashboard](#dashboard)
- [How to Run](#how-to-run)
- [CI/CD Pipeline](#cicd-pipeline)
- [Key Findings](#key-findings)

---

## Project Summary

This project demonstrates a complete data engineering workflow from raw data to business insights. It was built to showcase real-world skills including ETL pipeline development, SQL transformation with dbt, automated testing, containerisation with Docker, and business intelligence with Tableau.

**Data source:** Olist Brazilian E-Commerce Public Dataset (Kaggle) — 100,000 real orders from 2016–2018 across 9 related tables.

**Business questions answered:**
- What are the monthly and quarterly revenue trends?
- Which product categories generate the most revenue?
- Which Brazilian states have the most customers and highest order values?
- What is the average delivery time and how does it vary by state?
- Which payment methods do customers prefer?

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA PIPELINE                            │
│                                                                 │
│  Kaggle CSVs  →  Python ETL  →  Postgres (raw)                  │
│                      ↓                                          │
│               dbt Staging Layer  →  Clean tables                │
│                      ↓                                          │
│               dbt Mart Layer    →  Business tables              │
│                      ↓                                          │
│               Tableau Dashboard →  Visual insights              │
│                                                                 │
│  GitHub Actions CI/CD runs on every push:                       │
│  Lint → Unit Tests → dbt Run → dbt Test                         │
└─────────────────────────────────────────────────────────────────┘
```

**Data flow:**

1. Raw CSV files are read by the Python ingestion script
2. Each table is validated — null checks, deduplication, type casting
3. Clean data is loaded into the `raw` schema in Postgres
4. dbt staging models clean and standardise each raw table
5. dbt mart models join and aggregate staging tables into business-ready tables
6. Tableau connects to the mart tables and renders the dashboard
7. Every code push triggers GitHub Actions to re-run all tests automatically

---

## Tech Stack

| Tool | Version | Purpose |
|---|---|---|
| Python | 3.11 | Data ingestion, validation, loading |
| pandas | 2.2.0 | CSV reading, data manipulation |
| SQLAlchemy | 2.0.25 | Database connection and ORM |
| PostgreSQL | 15 | Data warehouse |
| dbt Core | 1.11 | SQL transformations and testing |
| Docker | Latest | Containerisation of the full pipeline |
| Docker Compose | Latest | Multi-container orchestration |
| GitHub Actions | Latest | CI/CD automated testing |
| pytest | 8.0.0 | Python unit testing |
| flake8 | Latest | Python linting |
| Tableau Public | Latest | Dashboard and visualisation |

---

## Dataset

**Source:** [Olist Brazilian E-Commerce Public Dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

| Table | Rows | Description |
|---|---|---|
| olist_orders | 99,441 | Orders with status and timestamps |
| olist_order_items | 112,650 | Line items — product, seller, price |
| olist_customers | 99,441 | Customer location data |
| olist_products | 32,951 | Product details and categories |
| olist_order_payments | 103,886 | Payment method and value |
| olist_order_reviews | 98,410 | Customer review scores |
| olist_sellers | 3,095 | Seller location data |
| olist_geolocation | 1,000,163 | Brazilian zip code coordinates |
| product_category_translation | 71 | Portuguese to English category names |

**Total rows ingested: 1,549,707**

---

## Pipeline Phases

### Phase 1 — Data Ingestion (`ingestion/extract.py`)

The Python ingestion script reads all 9 CSV files and loads them into Postgres with full validation:

- Required column checks — raises an error if expected columns are missing
- Null counting — logs warnings for null values in key fields
- Deduplication — removes duplicate primary keys and logs the count
- Empty row removal — drops fully empty rows
- Whitespace stripping — trims all string columns
- Metadata stamping — adds `_ingested_at` and `_source_file` columns to every table
- Structured logging — timestamped logs written to file and console
- CI/CD compatible — exits with code 1 if any table fails, turning the pipeline red on GitHub

```bash
python ingestion/extract.py
```

### Phase 2 — dbt Transformations (`dbt_project/ecommerce_dbt/models/`)

The transformation layer is split into two layers:

**Staging layer** — one model per raw table, handles cleaning and type casting:

| Model | Description |
|---|---|
| `stg_orders` | Casts timestamps, computes delivery days and on-time flag |
| `stg_order_items` | Casts price and freight to numeric, computes total item value |
| `stg_customers` | Renames columns to consistent naming convention |
| `stg_products` | Joins with translation table to add English category names |
| `stg_order_payments` | Casts payment values and installments |

**Mart layer** — business-ready aggregated tables:

| Model | Description |
|---|---|
| `mart_sales_summary` | One row per order with revenue, customer, delivery, and payment data joined |
| `mart_product_performance` | Revenue and volume metrics aggregated by product category |
| `mart_delivery_performance` | Average delivery days and on-time rate aggregated by Brazilian state |

```bash
cd dbt_project/ecommerce_dbt
dbt run
dbt test
```

### Phase 3 — CI/CD (`github/workflows/ci.yml`)

Every push to the `main` or `develop` branch automatically triggers:

1. Python 3.11 environment setup
2. Dependency installation from `requirements.txt`
3. flake8 linting check
4. pytest unit tests (6 tests covering validation logic)
5. dbt run against a fresh test Postgres database
6. dbt test to verify data quality

### Phase 4 — Docker (`Dockerfile`, `docker-compose.yml`)

The entire pipeline is containerised. Two services run together:

- `ecommerce-db` — PostgreSQL 15 database with a persistent volume
- `ecommerce-pipeline` — Python container that runs the ingestion script

```bash
docker compose up --build
```

This single command starts the database, waits for it to be healthy, then runs the full ingestion automatically.

### Phase 5 — Tableau Dashboard

The Tableau dashboard connects directly to the mart tables in Postgres and visualises:

- Monthly revenue trend line chart
- Top 15 product categories by revenue (bar chart)
- Delivery performance by state (choropleth map)
- Payment type breakdown (pie chart)
- KPI summary cards — total revenue, total orders, average order value, average delivery days

---

## Project Structure

```
olist-data-pipeline/
├── .github/
│   └── workflows/
│       └── ci.yml                    ← GitHub Actions CI/CD pipeline
├── ingestion/
│   ├── extract.py                    ← Python ETL ingestion script
│   └── requirements.txt              ← Python dependencies
├── dbt_project/
│   └── ecommerce_dbt/
│       ├── dbt_project.yml           ← dbt project configuration
│       └── models/
│           ├── staging/
│           │   ├── sources.yml       ← raw schema source definitions
│           │   ├── stg_orders.sql
│           │   ├── stg_order_items.sql
│           │   ├── stg_customers.sql
│           │   ├── stg_products.sql
│           │   └── stg_order_payments.sql
│           └── marts/
│               ├── schema.yml        ← dbt data quality tests
│               ├── mart_sales_summary.sql
│               ├── mart_product_performance.sql
│               └── mart_delivery_performance.sql
├── tests/
│   └── test_extract.py               ← pytest unit tests
├── ci_seed.py                        ← CI database seeding script
├── Dockerfile                        ← Pipeline container definition
├── docker-compose.yml                ← Multi-container orchestration
├── .env.example                      ← Environment variable template
├── .gitignore
└── README.md
```

---

## Dashboard

**[View Live Dashboard on Tableau Public](#)**
*https://public.tableau.com/app/profile/mohammed.umar5385/viz/Ecommerce_v2025_3/Dashboard2?publish=yes*

The dashboard answers the key business questions using three mart tables:

- **mart_sales_summary** powers the revenue trend, KPI cards, and payment type chart
- **mart_product_performance** powers the category revenue bar chart
- **mart_delivery_performance** powers the state delivery map

---

## How to Run

### Prerequisites

- Docker Desktop installed and running
- Git installed
- Kaggle account to download the dataset

### Step 1 — Clone the repository

```bash
git clone https://github.com/mumar112/olist-data-pipeline.git
cd olist-data-pipeline
```

### Step 2 — Download the dataset

Go to [kaggle.com/datasets/olistbr/brazilian-ecommerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and download the zip. Extract all 9 CSV files into `data/raw/`.

### Step 3 — Set up environment variables

```bash
cp .env.example .env
```

Edit `.env` if needed — the defaults work with Docker Compose out of the box.

### Step 4 — Run the full pipeline

```bash
docker compose up --build
```

This starts Postgres, waits for it to be healthy, then runs the ingestion script automatically. You will see all 9 tables load with row counts in the terminal.

### Step 5 — Run dbt transformations

```bash
cd dbt_project/ecommerce_dbt
dbt run
dbt test
```

### Step 6 — Run tests locally

```bash
python -m pytest tests/ -v
```

---

## CI/CD Pipeline

The GitHub Actions workflow runs automatically on every push:

```
Push to main
    ↓
Set up Python 3.11
    ↓
Install dependencies
    ↓
flake8 lint check
    ↓
pytest unit tests (6 tests)
    ↓
Install dbt
    ↓
Seed test database with empty raw tables
    ↓
dbt run (8 models)
    ↓
dbt test (12 data quality tests)
    ↓
✅ Green checkmark on GitHub
```

---

## Key Findings

- Credit card is the dominant payment method, used in over 73% of all orders
- Health and beauty, watches and gifts, and bed/bath/table are the top three revenue categories
- São Paulo state accounts for the largest share of both customers and orders
- Average delivery time across Brazil is approximately 12 days, with the North region taking significantly longer
- Orders delivered on time have noticeably higher review scores than late deliveries

---

## Contact

**Mohammed Umar** — Data Engineering Portfolio Project

[GitHub](https://github.com/mumar112) · https://www.linkedin.com/in/mohammed-umar-122577209/ · https://public.tableau.com/app/profile/mohammed.umar5385/viz/Ecommerce_v2025_3/Dashboard2?publish=yes

---

*This project was built as a portfolio piece demonstrating end-to-end data engineering skills including Python ETL, SQL transformation with dbt, CI/CD automation, Docker containerisation, and business intelligence with Tableau.*
