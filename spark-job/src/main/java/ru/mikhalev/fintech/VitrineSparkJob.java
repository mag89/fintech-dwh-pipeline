package ru.mikhalev.fintech;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class VitrineSparkJob {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("FintechStarSchemaETL")
                .master("local[*]")
                .getOrCreate();

        String jdbcUrl = System.getenv().getOrDefault("DB_URL", "jdbc:postgresql://postgres:5432/airflow");
        Properties props = new Properties();
        props.setProperty("user", System.getenv().getOrDefault("DB_USER", "airflow"));
        props.setProperty("password", System.getenv().getOrDefault("DB_PASSWORD", "airflow"));
        props.setProperty("driver", "org.postgresql.Driver");

        // 1. Читаем ТОЛЬКО новые строки из Staging (Predicate Pushdown на сторону Postgres)
        String incrementalQuery = "(SELECT * FROM raw_transactions r " +
                "WHERE NOT EXISTS (SELECT 1 FROM fact_transactions f WHERE f.tx_id = r.tx_id)) AS new_raw";

        Dataset<Row> newRawDf = spark.read()
                .jdbc(jdbcUrl, incrementalQuery, props)
                .persist(StorageLevel.MEMORY_AND_DISK()); // Кешируем, чтобы не читать из БД 3 раза

        // Если новых данных нет — завершаем работу без лишней нагрузки
        if (newRawDf.isEmpty()) {
            newRawDf.unpersist();
            spark.stop();
            return;
        }

        // 2. Обновляем измерение дат (dim_date)
        Dataset<Row> existingDates = spark.read().jdbc(jdbcUrl, "dim_date", props);
        Dataset<Row> newDates = newRawDf
                .select(to_date(col("created_at")).alias("full_date"))
                .distinct()
                .join(broadcast(existingDates), "full_date", "left_anti")
                .withColumn("date_key", date_format(col("full_date"), "yyyyMMdd").cast("int"))
                .withColumn("year_num", year(col("full_date")))
                .withColumn("month_num", month(col("full_date")))
                .withColumn("is_weekend", dayofweek(col("full_date")).isin(1, 7));

        if (!newDates.isEmpty()) {
            newDates.write().mode(SaveMode.Append).jdbc(jdbcUrl, "dim_date", props);
        }

        // 3. Обновляем измерение клиентов (dim_user)
        Dataset<Row> existingUsers = spark.read().jdbc(jdbcUrl, "dim_user", props);
        Dataset<Row> newUsers = newRawDf
                .select(col("user_id"))
                .distinct()
                .join(broadcast(existingUsers), "user_id", "left_anti")
                .withColumn("user_segment", when(col("user_id").mod(10).equalTo(0), "VIP").otherwise("RETAIL"));

        if (!newUsers.isEmpty()) {
            newUsers.write().mode(SaveMode.Append).jdbc(jdbcUrl, "dim_user", props);
        }

        // 4. Собираем факты через Broadcast Hash Join со справочниками (без сетевого Shuffle)
        Dataset<Row> dimDate = broadcast(spark.read().jdbc(jdbcUrl, "dim_date", props));
        Dataset<Row> dimCat = broadcast(spark.read().jdbc(jdbcUrl, "dim_merchant_category", props));
        Dataset<Row> dimUser = broadcast(spark.read().jdbc(jdbcUrl, "dim_user", props));

        Dataset<Row> factsToLoad = newRawDf
                .withColumn("full_date", to_date(col("created_at")))
                .join(dimDate, "full_date", "inner")
                .join(dimCat, newRawDf.col("merchant_category").equalTo(dimCat.col("category_code")), "inner")
                .join(dimUser, "user_id", "inner")
                .select(
                        col("tx_id"),
                        col("date_key"),
                        col("category_key"),
                        col("user_key"),
                        col("amount"),
                        col("is_suspicious")
                );

        factsToLoad.write().mode(SaveMode.Append).jdbc(jdbcUrl, "fact_transactions", props);

        newRawDf.unpersist();
        spark.stop();
    }
}
