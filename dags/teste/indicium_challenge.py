import airflow
import psycopg2
import os
import sqlalchemy

from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

POSTGRESS_DIR = 'postgres_data'
CSV_DIR = 'csv_data'


def create_csv_from_tables():
    connection = psycopg2.connect(host="db", database="northwind", user="northwind_user",
                                  password="thewindisblowing")
    cursor = connection.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")

    if not os.path.exists(POSTGRESS_DIR):
        os.makedirs(POSTGRESS_DIR)
    for table in cursor.fetchall():
        table_name = ''.join(table)
        dataframe = pd.read_sql("select * from " + table_name, connection)
        dataframe.to_csv(os.path.join(
            POSTGRESS_DIR, table_name+".csv"), index=False)


def extract_csv():
    df = pd.read_csv("/data/order_details.csv")
    if not os.path.exists(CSV_DIR):
        os.makedirs(CSV_DIR)
    df.to_csv(os.path.join(
        CSV_DIR, "details.csv"), index=False)


def merge_and_write():
    df_orders = pd.read_csv(POSTGRESS_DIR + "/orders.csv")
    df_products = pd.read_csv(
        POSTGRESS_DIR + "/products.csv")
    df_products = df_products[["product_id", "product_name"]]
    df_orders = df_orders["order_id"]
    df_details = pd.read_csv(CSV_DIR + "/details.csv")
    df_orders_details = pd.merge(df_orders, df_details, on="order_id")
    df_orders_details = df_orders_details.merge(
        df_products, on="product_id")
    engine = create_engine(
        'postgresql+psycopg2://target_user:foo@target/target')
    df_orders_details.to_sql('order_detail', engine, if_exists='append', index=False)


def print_final():
    connection = psycopg2.connect(host="target", database="target", user="target_user",
                                  password="foo")
    dataframe = pd.read_sql("select * from order_detail", connection)
    print(dataframe)


default_args = {
    'owner': 'ruizerinha',
}

indicium_challenge = DAG(
    'indicium_challenge',
    default_args=default_args,
    start_date=datetime(2023, 1, 18)
)

task_1 = PythonOperator(
    task_id='create_csv_from_tables',
    python_callable=create_csv_from_tables,
    dag=indicium_challenge
)

task_2 = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_csv,
    dag=indicium_challenge
)

task_3 = PythonOperator(
    task_id='merge_and_write',
    python_callable=merge_and_write,
    dag=indicium_challenge
)

task_4 = PythonOperator(
    task_id='print_final',
    python_callable=print_final,
    dag=indicium_challenge
)

task_1 >> task_3
task_2 >> task_3
task_3 >> task_4
