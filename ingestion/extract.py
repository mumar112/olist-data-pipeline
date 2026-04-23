"""
extract.py — E-commerce Pipeline: Data Ingestion
Reads 9 Olist Kaggle CSV files, validates each one,
and loads them into the raw schema in Postgres or BigQuery.
 
Usage:  python ingestion/extract.py
"""
 
import os, sys, logging
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
 
load_dotenv()
 
# ── Config ──────────────────────────────────────────────────
RAW_DIR      = os.getenv("RAW_DIR", "data/raw")
DESTINATION  = os.getenv("DESTINATION", "postgres")
DATABASE_URL = os.getenv("DATABASE_URL")
GCP_PROJECT  = os.getenv("GCP_PROJECT")
BQ_DATASET   = os.getenv("BQ_DATASET", "raw")
 
# ── Logging ─────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingestion/ingestion.log")
    ]
)
log = logging.getLogger(__name__)
 
# ── Table definitions ────────────────────────────────────────
# Each entry: file name, required columns, date columns, PK
TABLES = {
    "raw_orders": {
        "file": "olist_orders_dataset.csv",
        "required": ["order_id","customer_id","order_status",
                     "order_purchase_timestamp"],
        "dates": ["order_purchase_timestamp","order_approved_at",
                  "order_delivered_carrier_date",
                  "order_delivered_customer_date",
                  "order_estimated_delivery_date"],
        "pk": "order_id",
    },
    "raw_order_items": {
        "file": "olist_order_items_dataset.csv",
        "required": ["order_id","product_id","price","freight_value"],
        "dates": ["shipping_limit_date"],
        "pk": None,
    },
    "raw_customers": {
        "file": "olist_customers_dataset.csv",
        "required": ["customer_id","customer_unique_id","customer_state"],
        "dates": [],
        "pk": "customer_id",
    },
    "raw_products": {
        "file": "olist_products_dataset.csv",
        "required": ["product_id","product_category_name"],
        "dates": [],
        "pk": "product_id",
    },
    "raw_order_payments": {
        "file": "olist_order_payments_dataset.csv",
        "required": ["order_id","payment_type","payment_value"],
        "dates": [],
        "pk": None,
    },
    "raw_order_reviews": {
        "file": "olist_order_reviews_dataset.csv",
        "required": ["review_id","order_id","review_score"],
        "dates": ["review_creation_date","review_answer_timestamp"],
        "pk": "review_id",
    },
    "raw_sellers": {
        "file": "olist_sellers_dataset.csv",
        "required": ["seller_id","seller_state"],
        "dates": [],
        "pk": "seller_id",
    },
    "raw_geolocation": {
        "file": "olist_geolocation_dataset.csv",
        "required": ["geolocation_zip_code_prefix","geolocation_lat",
                     "geolocation_lng","geolocation_state"],
        "dates": [],
        "pk": None,
    },
    "raw_category_translation": {
        "file": "product_category_name_translation.csv",
        "required": ["product_category_name",
                     "product_category_name_english"],
        "dates": [],
        "pk": "product_category_name",
    },
}
 
# ── Validation ───────────────────────────────────────────────
def validate(df, table_name, cfg):
    log.info(f'  Validating {table_name} ({len(df):,} rows)...')
 
    # 1. Required columns must exist
    missing = [c for c in cfg['required'] if c not in df.columns]
    if missing:
        raise ValueError(f'Missing columns in {table_name}: {missing}')
 
    # 2. Check nulls in required columns
    for col in cfg['required']:
        n = df[col].isnull().sum()
        if n > 0:
            log.warning(f'    NULL: {n:,} nulls in {col}')
 
    # 3. Deduplicate on primary key if defined
    if cfg['pk'] and cfg['pk'] in df.columns:
        dupes = df[cfg['pk']].duplicated().sum()
        if dupes > 0:
            log.warning(f'    DEDUP: {dupes:,} duplicate {cfg["pk"]} — dropping')
            df = df.drop_duplicates(subset=[cfg['pk']], keep='first')
 
    # 4. Drop fully empty rows
    empty = df.isnull().all(axis=1).sum()
    if empty > 0:
        log.warning(f'    EMPTY ROWS: {empty:,} dropped')
        df = df.dropna(how='all')
 
    # 5. Strip whitespace from string columns
    for col in df.select_dtypes('object').columns:
        df[col] = df[col].str.strip()
 
    # 6. Add metadata columns
    df['_ingested_at'] = datetime.utcnow()
    df['_source_file'] = cfg['file']
 
    log.info(f'  Validation passed — {len(df):,} rows ready')
    return df
 
# ── Loaders ──────────────────────────────────────────────────
def load_postgres(df, table_name):
    from sqlalchemy import create_engine, text
    engine = create_engine(DATABASE_URL)
    with engine.connect() as conn:
        conn.execute(text('CREATE SCHEMA IF NOT EXISTS raw'))
        conn.commit()
    df.to_sql(table_name, engine, schema='raw',
              if_exists='replace', index=False,
              chunksize=5000, method='multi')
    log.info(f'  Loaded {len(df):,} rows -> raw.{table_name} (Postgres)')
 
def load_bigquery(df, table_name):
    from google.cloud import bigquery
    client = bigquery.Client(project=GCP_PROJECT)
    dest = f'{GCP_PROJECT}.{BQ_DATASET}.{table_name}'
    cfg = bigquery.LoadJobConfig(
        write_disposition='WRITE_TRUNCATE', autodetect=True)
    client.load_table_from_dataframe(df, dest,
        job_config=cfg).result()
    log.info(f'  Loaded {len(df):,} rows -> {dest} (BigQuery)')
 
# ── Main ingestion loop ──────────────────────────────────────
def ingest_table(table_name, cfg):
    filepath = os.path.join(RAW_DIR, cfg['file'])
    if not os.path.exists(filepath):
        log.error(f'  FILE NOT FOUND: {filepath} — skipping')
        return {'table': table_name, 'status': 'skipped', 'rows': 0}
 
    log.info(f'\nIngesting {cfg["file"]}...')
    df = pd.read_csv(
        filepath,
        parse_dates=cfg['dates'] if cfg['dates'] else False,
        low_memory=False
    )
    log.info(f'  Read {len(df):,} rows, {len(df.columns)} columns')
 
    df = validate(df, table_name, cfg)
 
    if DESTINATION == 'bigquery': load_bigquery(df, table_name)
    else:                         load_postgres(df, table_name)
 
    return {'table': table_name, 'status': 'success', 'rows': len(df)}
 
def run():
    log.info('=' * 55)
    log.info('ECOMMERCE PIPELINE — DATA INGESTION')
    log.info(f'Destination : {DESTINATION.upper()}')
    log.info(f'Source dir  : {RAW_DIR}')
    log.info('=' * 55)
    start = datetime.utcnow()
    results = []
 
    for name, cfg in TABLES.items():
        try:
            results.append(ingest_table(name, cfg))
        except Exception as e:
            log.error(f'  FAILED: {name} — {e}')
            results.append({'table':name,'status':'failed','rows':0})
 
    elapsed = (datetime.utcnow()-start).total_seconds()
    log.info('\n' + '=' * 55)
    log.info('INGESTION SUMMARY')
    log.info('=' * 55)
    for r in results:
        icon = 'OK' if r['status']=='success' else '!!'
        log.info(f"  [{icon}] {r['table']:<42} {r['rows']:>8,} rows")
    log.info(f'\n  Completed in {elapsed:.1f}s')
    log.info('=' * 55)
 
    if any(r['status']=='failed' for r in results):
        sys.exit(1)   # non-zero exit code fails CI/CD
 
if __name__ == '__main__':
    run()
