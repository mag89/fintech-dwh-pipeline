from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'artur.mikhalev',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='fintech_daily_vitrine_pipeline',
    default_args=default_args,
    description='Оркестрация витрины транзакций (Kafka -> Postgres -> Java Spark Job)',
    schedule='@daily',
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=['java', 'spark', 'kafka'],
) as dag:

    ingest_kafka_batch = BashOperator(
        task_id='ingest_kafka_batch',
        bash_command=(
            'DB_HOST=postgres KAFKA_HOST=kafka '
            'java -jar /opt/airflow/kafka-stream-service/target/kafka-stream-service-1.0.0.jar'
        )
    )

    prepare_vitrine_table = SQLExecuteQueryOperator(
        task_id='prepare_vitrine_table',
        conn_id='fintech_postgres_conn',
        sql=[
            """
            CREATE TABLE IF NOT EXISTS dim_merchant_category (
                category_key SERIAL PRIMARY KEY,
                category_code VARCHAR(50) UNIQUE NOT NULL,
                business_group VARCHAR(50) NOT NULL
            );
            """,
            """
            INSERT INTO dim_merchant_category (category_code, business_group) VALUES
                ('SUPERMARKETS', 'ESSENTIAL_GOODS'),
                ('TRANSPORT', 'ESSENTIAL_GOODS'),
                ('IT_SERVICES', 'DIGITAL_AND_TECH'),
                ('RESTAURANTS', 'LIFESTYLE')
            ON CONFLICT (category_code) DO NOTHING;
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INT PRIMARY KEY,
                full_date DATE UNIQUE NOT NULL,
                year_num INT NOT NULL,
                month_num INT NOT NULL,
                is_weekend BOOLEAN NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS dim_user (
                user_key SERIAL PRIMARY KEY,
                user_id BIGINT UNIQUE NOT NULL,
                user_segment VARCHAR(30) NOT NULL
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS fact_transactions (
                tx_id VARCHAR(36) PRIMARY KEY,
                date_key INT REFERENCES dim_date(date_key),
                category_key INT REFERENCES dim_merchant_category(category_key),
                user_key INT REFERENCES dim_user(user_key),
                amount NUMERIC(15, 2) NOT NULL,
                is_suspicious BOOLEAN NOT NULL
            );
            """
        ]
    )

    run_java_spark_job = BashOperator(
        task_id='run_java_spark_job',
        bash_command=(
            'java '
            '--add-opens=java.base/java.lang=ALL-UNNAMED '
            '--add-opens=java.base/java.lang.invoke=ALL-UNNAMED '
            '--add-opens=java.base/java.lang.reflect=ALL-UNNAMED '
            '--add-opens=java.base/java.io=ALL-UNNAMED '
            '--add-opens=java.base/java.net=ALL-UNNAMED '
            '--add-opens=java.base/java.nio=ALL-UNNAMED '
            '--add-opens=java.base/java.util=ALL-UNNAMED '
            '--add-opens=java.base/java.util.concurrent=ALL-UNNAMED '
            '--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED '
            '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED '
            '--add-opens=java.base/sun.nio.cs=ALL-UNNAMED '
            '--add-opens=java.base/sun.security.action=ALL-UNNAMED '
            '--add-opens=java.base/sun.util.calendar=ALL-UNNAMED '
            '-jar /opt/airflow/spark-job/target/analytics-job.jar'
        )
    )

    ingest_kafka_batch >> prepare_vitrine_table >> run_java_spark_job
