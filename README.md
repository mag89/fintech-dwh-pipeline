End-to-end ETL/ELT-конвейер обработки финансовых транзакций с потоковым приёмом в Staging-слой и пакетной трансформацией в корпоративное хранилище данных (DWH) по схеме **«Звезда» (Star Schema)**.

## Архитектура решения

```text
                [Apache Airflow 3.4: DAG fintech_daily_vitrine_pipeline]
                                           │
       ┌───────────────────────────────────┼───────────────────────────────────┐
       ▼                                   ▼                                   ▼
[1. ingest_kafka_batch]         [2. prepare_vitrine_table]         [3. run_java_spark_job]
Spring Boot 3.4 (Java 17)         PostgreSQL 16 (DDL/DML)            Apache Spark 3.5 (Java 17)
  ├── Producer (JSON events)        ├── dim_merchant_category          ├── Incremental Load (Pushdown)
  ├── Apache Kafka (KRaft)          ├── dim_date                       ├── Broadcast Hash Join
  └── Consumer -> Staging           ├── dim_user                       └── Загрузка fact_transactions
       │                            └── fact_transactions                      │
       ▼                                                                       ▼
[PostgreSQL: raw_transactions] ───────────────────────────────────► [Core DWH: Star Schema]
```

```text
├── dags/
│   └── fintech_pipeline_dag.py     # Оркестрация конвейера в Apache Airflow 3.4
├── kafka-stream-service/           # Модуль Spring Boot 3.4 (Kafka Producer/Consumer + JUnit тесты)
├── spark-job/                      # Модуль Apache Spark 3.5 (Java ETL для наполнения Star Schema)
├── Dockerfile                      # Кастомный образ Airflow 3.4 с установленной Java 17 JRE
├── docker-compose.yaml             # Инфраструктура: Airflow (Celery), Postgres 16, Redis, Kafka 3.8
└── README.md
```

## Технологический стек

* **Язык и фреймворки:** Java 17, Spring Boot 3.4 (Spring Kafka, Spring JDBC), Apache Spark 3.5 (Spark SQL / Dataset API)
* **Брокер и оркестрация:** Apache Kafka 3.8, Apache Airflow 3.4 
* **Хранение данных:** PostgreSQL 16 (Star Schema DWH)
* **Инфраструктура и тестирование:** Docker, Docker Compose, Maven, JUnit 5, Mockito


## Быстрый запуск

1. **Собрать JAR-артефакты модулей (через Docker без локальной установки Maven):**
   ```bash
   docker run --rm -v "\$(pwd)/kafka-stream-service":/app -w /app maven:3.9-eclipse-temurin-17 mvn clean package
   docker run --rm -v "\$(pwd)/spark-job":/app -w /app maven:3.9-eclipse-temurin-17 mvn clean package
   ```
2. **Собрать образ Airflow с Java 17 и поднять всю инфраструктуру:**
   ```bash
   docker compose build
   docker compose up airflow-init
   docker compose up -d
   ```
3. **Запустить пайплайн в Airflow UI (`http://localhost:8080`, логин/пароль `airflow` / `airflow`):**
   * В разделе **Admin → Connections** создать подключение `fintech_postgres_conn` (Host: `postgres`, DB/Login/Password: `airflow`, Port: `5432`).
   * Включить и запустить DAG **`fintech_daily_vitrine_pipeline`**.
4. **Проверить итоговую витрину по схеме «Звезда»:**
   ```bash
   docker compose exec postgres psql -U airflow -d airflow -c "
   SELECT d.full_date, c.business_group, c.category_code, u.user_segment,
          COUNT(f.tx_id) AS tx_cnt, SUM(f.amount) AS total_sum
   FROM fact_transactions f
   JOIN dim_date d ON f.date_key = d.date_key
   JOIN dim_merchant_category c ON f.category_key = c.category_key
   JOIN dim_user u ON f.user_key = u.user_key
   GROUP BY 1, 2, 3, 4;"
   ```