import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from pymongo import MongoClient, errors as mongo_errors
from datetime import datetime
import time
import logging

# Configuração do logger
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger()

def wait_for_services(max_retries=10, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            # Testa conexão com PostgreSQL
            pg_conn = psycopg2.connect(
                host="postgres",
                dbname="pp_dd",
                user="postgres",
                password="postgres"
            )
            pg_conn.close()

            # Testa conexão com MongoDB
            mongo_client = MongoClient("mongodb://mongo:27017/", serverSelectionTimeoutMS=3000)
            mongo_client.admin.command('ping')
            mongo_client.close()

            logger.info("Todos os serviços estão prontos.")
            return
        except (OperationalError, mongo_errors.PyMongoError) as e:
            logger.warning(f"Tentativa {attempt}/{max_retries}: aguardando serviços... ({e})")
            time.sleep(delay)

    raise Exception("Serviços não responderam dentro do tempo esperado.")

def create_dimensional_model(cursor):
    commands = [
        """
        CREATE TABLE IF NOT EXISTS dim_customer (
            customer_id VARCHAR PRIMARY KEY,
            customer_unique_id VARCHAR,
            customer_zip_code_prefix VARCHAR,
            customer_city VARCHAR,
            customer_state VARCHAR
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_product (
            product_id VARCHAR PRIMARY KEY,
            product_category_name VARCHAR,
            product_name_length INTEGER,
            product_description_length INTEGER,
            product_photos_qty INTEGER,
            product_weight_g INTEGER,
            product_length_cm INTEGER,
            product_height_cm INTEGER,
            product_width_cm INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_date (
            date_id SERIAL PRIMARY KEY,
            date DATE UNIQUE,
            year INTEGER,
            month INTEGER,
            day INTEGER
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS dim_payment_type (
            payment_type_id SERIAL PRIMARY KEY,
            payment_type VARCHAR UNIQUE
        );
        """,
        """
        CREATE TABLE IF NOT EXISTS fact_sales (
            sale_id SERIAL PRIMARY KEY,
            order_id VARCHAR,
            customer_id VARCHAR REFERENCES dim_customer(customer_id),
            product_id VARCHAR REFERENCES dim_product(product_id),
            date_id INTEGER REFERENCES dim_date(date_id),
            payment_type_id INTEGER REFERENCES dim_payment_type(payment_type_id),
            review_score INTEGER,
            price NUMERIC(10,2),
            freight_value NUMERIC(10,2),
            payment_installments INTEGER,
            payment_value NUMERIC(10,2)
        );
        """
    ]
    for command in commands:
        cursor.execute(command)

def load_and_transform_data():
    customers = pd.read_csv("input/olist_customers_dataset.csv")
    orders = pd.read_csv("input/olist_orders_dataset.csv", parse_dates=['order_purchase_timestamp'])
    order_items = pd.read_csv("input/olist_order_items_dataset.csv")
    products = pd.read_csv("input/olist_products_dataset.csv")
    payments = pd.read_csv("input/olist_order_payments_dataset.csv")

    client = MongoClient("mongodb://mongo:27017/")
    reviews_cursor = client['ecommerce']['order_reviews'].find()
    reviews = pd.DataFrame(list(reviews_cursor))

    return customers, orders, order_items, products, payments, reviews

def load_dimensions(cursor, customers, products, payments, orders):
    for _, row in customers.drop_duplicates(subset=['customer_id']).iterrows():
        cursor.execute("""
            INSERT INTO dim_customer (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (customer_id) DO NOTHING
        """, (
            row['customer_id'],
            row['customer_unique_id'],
            row['customer_zip_code_prefix'],
            row['customer_city'],
            row['customer_state']
        ))

    for _, row in products.drop_duplicates(subset=['product_id']).iterrows():
        values = (
            row['product_id'],
            row.get('product_category_name'),
            int(row['product_name_length']) if pd.notna(row.get('product_name_length')) else None,
            int(row['product_description_length']) if pd.notna(row.get('product_description_length')) else None,
            int(row['product_photos_qty']) if pd.notna(row.get('product_photos_qty')) else None,
            int(row['product_weight_g']) if pd.notna(row.get('product_weight_g')) else None,
            int(row['product_length_cm']) if pd.notna(row.get('product_length_cm')) else None,
            int(row['product_height_cm']) if pd.notna(row.get('product_height_cm')) else None,
            int(row['product_width_cm']) if pd.notna(row.get('product_width_cm')) else None
        )
        cursor.execute("""
            INSERT INTO dim_product (
                product_id, product_category_name, product_name_length, product_description_length,
                product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (product_id) DO NOTHING
        """, values)

    for payment_type in payments['payment_type'].dropna().drop_duplicates():
        cursor.execute("""
            INSERT INTO dim_payment_type (payment_type)
            VALUES (%s)
            ON CONFLICT (payment_type) DO NOTHING
        """, (payment_type,))

    unique_dates = pd.to_datetime(orders['order_purchase_timestamp'].dropna()).dt.date.unique()
    for d in unique_dates:
        dt = pd.to_datetime(d)
        cursor.execute("""
            INSERT INTO dim_date (date, year, month, day)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (date) DO NOTHING
        """, (dt.date(), dt.year, dt.month, dt.day))

def load_fact_sales(cursor, orders, order_items, customers, products, payments, reviews):
    df = (
        orders.merge(order_items, on='order_id', how='inner')
              .merge(customers, on='customer_id', how='inner')
              .merge(products, on='product_id', how='inner')
              .merge(payments, on='order_id', how='inner')
              .merge(reviews[['order_id', 'review_score']], on='order_id', how='left')
    )

    for _, row in df.iterrows():
        try:
            order_date = row['order_purchase_timestamp'].date()
            cursor.execute("SELECT date_id FROM dim_date WHERE date = %s", (order_date,))
            date_result = cursor.fetchone()

            cursor.execute("SELECT payment_type_id FROM dim_payment_type WHERE payment_type = %s", (row['payment_type'],))
            payment_type_result = cursor.fetchone()

            if date_result and payment_type_result:
                cursor.execute("""
                    INSERT INTO fact_sales (
                        order_id, customer_id, product_id, date_id, payment_type_id,
                        review_score, price, freight_value, payment_installments, payment_value
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    row['order_id'],
                    row['customer_id'],
                    row['product_id'],
                    date_result[0],
                    payment_type_result[0],
                    int(row['review_score']) if pd.notna(row['review_score']) else None,
                    float(row['price']),
                    float(row['freight_value']),
                    int(row['payment_installments']) if pd.notna(row['payment_installments']) else None,
                    float(row['payment_value'])
                ))
        except Exception as e:
            logger.error(f"Erro ao inserir venda - Order ID {row['order_id']}: {e}")

def main():
    try:
        wait_for_services()

        customers, orders, order_items, products, payments, reviews = load_and_transform_data()

        conn = psycopg2.connect(
            host="postgres",
            dbname="pp_dd",
            user="postgres",
            password="postgres"
        )
        cursor = conn.cursor()

        logger.info("Criando modelo dimensional...")
        create_dimensional_model(cursor)
        conn.commit()

        logger.info("Carregando dimensões...")
        load_dimensions(cursor, customers, products, payments, orders)
        conn.commit()

        logger.info("Carregando fatos...")
        load_fact_sales(cursor, orders, order_items, customers, products, payments, reviews)
        conn.commit()

        logger.info("ETL concluído com sucesso!")

    except Exception as e:
        logger.exception(f"Erro no processo ETL: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    main()
