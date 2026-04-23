import os
import sys
import psycopg2

if "--profiles-only" in sys.argv:
    os.makedirs(os.path.expanduser("~/.dbt"), exist_ok=True)
    with open(os.path.expanduser("~/.dbt/profiles.yml"), "w") as f:
        f.write(
            "ecommerce_dbt:\n"
            "  target: ci\n"
            "  outputs:\n"
            "    ci:\n"
            "      type: postgres\n"
            "      host: localhost\n"
            "      user: postgres\n"
            "      password: password\n"
            "      port: 5432\n"
            "      dbname: ecommerce_test\n"
            "      schema: dbt_ci\n"
            "      threads: 2\n"
        )
    print("profiles.yml written")
    sys.exit(0)

conn = psycopg2.connect(
    host="localhost",
    dbname="ecommerce_test",
    user="postgres",
    password="password",
    port=5432
)
cur = conn.cursor()
cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_orders (
    order_id TEXT, customer_id TEXT, order_status TEXT,
    order_purchase_timestamp TEXT, order_approved_at TEXT,
    order_delivered_carrier_date TEXT,
    order_delivered_customer_date TEXT,
    order_estimated_delivery_date TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_order_items (
    order_id TEXT, order_item_id TEXT, product_id TEXT,
    seller_id TEXT, shipping_limit_date TEXT,
    price TEXT, freight_value TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_customers (
    customer_id TEXT, customer_unique_id TEXT,
    customer_zip_code_prefix TEXT,
    customer_city TEXT, customer_state TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_products (
    product_id TEXT, product_category_name TEXT,
    product_name_lenght TEXT, product_description_lenght TEXT,
    product_photos_qty TEXT, product_weight_g TEXT,
    product_length_cm TEXT, product_height_cm TEXT,
    product_width_cm TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_order_payments (
    order_id TEXT, payment_sequential TEXT,
    payment_type TEXT, payment_installments TEXT,
    payment_value TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_category_translation (
    product_category_name TEXT,
    product_category_name_english TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_order_reviews (
    review_id TEXT, order_id TEXT, review_score TEXT,
    review_creation_date TEXT, review_answer_timestamp TEXT)""")
cur.execute("""CREATE TABLE IF NOT EXISTS raw.raw_sellers (
    seller_id TEXT, seller_zip_code_prefix TEXT,
    seller_city TEXT, seller_state TEXT)""")
conn.commit()
cur.close()
conn.close()
print("All raw tables created successfully")