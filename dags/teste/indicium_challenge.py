import psycopg2
import os

from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine

POSTGRES_DIR = 'postgres_data'
CSV_DIR = 'csv_data'

SOURCE_HOST = "db"
SOURCE_DB = "northwind"
SOURCE_USER = "northwind_user"
SOURCE_PWD = "thewindisblowing"

SOURCE_CSV_DIR = "/data/order_details.csv"

TARGET_TABLE_NAME = "order_detail"
TARGET_HOST = "target"
TARGET_DB = "target"
TARGET_USER = "target_user"
TARGET_PWD = "foo"

START_DATE_YEAR = 2023
START_DATE_MONTH = 1
START_DATE_DAY = 1

TODAY = datetime.now()


def format_postgres_dir(day, month, year):
    return POSTGRES_DIR + "/" + str(year) + "-" + str(month) + "-" + str(day)


def create_csv_from_tables(**kwargs):
    connection = psycopg2.connect(host=SOURCE_HOST, database=SOURCE_DB, user=SOURCE_USER,
                                  password=SOURCE_PWD)
    cursor = connection.cursor()
    cursor.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    day = kwargs['params']['day']
    month = kwargs['params']['month']
    year = kwargs['params']['year']
    postgres_date_dir = format_postgres_dir(day=day, month=month, year=year)
    if not os.path.exists(POSTGRES_DIR):
        os.makedirs(POSTGRES_DIR)
    if not os.path.exists(postgres_date_dir):
        os.makedirs(postgres_date_dir)
    for table in cursor.fetchall():
        table_name = ''.join(table)
        dataframe = pd.read_sql("select * from " + table_name, connection)
        dataframe.to_csv(os.path.join(
            postgres_date_dir, table_name+".csv"), index=False)


def extract_csv():
    df = pd.read_csv(SOURCE_CSV_DIR)
    if not os.path.exists(CSV_DIR):
        os.makedirs(CSV_DIR)
    df.to_csv(os.path.join(
        CSV_DIR, "details.csv"), index=False)


def merge_and_write(**kwargs):
    day = kwargs['params']['day']
    month = kwargs['params']['month']
    year = kwargs['params']['year']
    postgres_date_dir = format_postgres_dir(day=day, month=month, year=year)
    df_orders = pd.read_csv(postgres_date_dir + "/orders.csv")
    df_orders = df_orders["order_id"]
    df_products = pd.read_csv(
        postgres_date_dir + "/products.csv")
    df_products = df_products[["product_id", "product_name"]]
    df_details = pd.read_csv(CSV_DIR + "/details.csv")

    df_orders_details = pd.merge(df_orders, df_details, on="order_id")
    df_orders_details = df_orders_details.merge(
        df_products, on="product_id")
    #TODO adicionar data de insercao no bd?
    engine = create_engine(
        'postgresql+psycopg2://' + TARGET_USER + ':' + TARGET_PWD + '@' + TARGET_HOST + '/' + TARGET_DB)
    df_orders_details.to_sql('order_detail', engine, if_exists='append', index=False)
    df_orders_details.to_sql(TARGET_TABLE_NAME, engine, if_exists='append', index=False)


def print_final():
    connection = psycopg2.connect(host=TARGET_HOST, database=TARGET_DB, user=TARGET_USER,
                                  password=TARGET_PWD)
    dataframe = pd.read_sql("select * from order_detail", connection)
    print(dataframe)

default_args = {
    'owner': 'the-people',
    'day': os.environ.get('DAY') if os.environ.get('DAY') else TODAY.day,
    'month': os.environ.get('MONTH') if os.environ.get('MONTH') else TODAY.month,
    'year': os.environ.get('YEAR') if os.environ.get('YEAR') else TODAY.year
}

indicium_challenge = DAG(
    'indicium_challenge',
    default_args=default_args,
    start_date=datetime(START_DATE_YEAR, START_DATE_MONTH, START_DATE_DAY),
    schedule_interval=timedelta(days=1)
)

task_1 = PythonOperator(
    task_id='create_csv_from_tables',
    python_callable=create_csv_from_tables,
    provide_context=True,
    params=default_args,
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
    provide_context=True,
    params=default_args,
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
