import os
import sys
import json
import warnings
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

warnings.filterwarnings("ignore")
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["hadoop.home.dir"] = "C:\\hadoop"
os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, when, window, sum as _sum, avg, count, max as _max, round, concat, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
)

CREDENTIALS_FILE = "credentials.json"
DATASET_ID = "ev_monitoring"
RAW_TABLE_ID = f"{DATASET_ID}.raw_ev_events"
AGG_TABLE_ID = f"{DATASET_ID}.agg_ev_metrics_5min"
INPUT_DIR = "./stream_input"
LOOKUP_FILE = "stations_lookup.csv"

with open(CREDENTIALS_FILE, "r") as f:
    key_data = json.load(f)
    PROJECT_ID = key_data["project_id"]

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CREDENTIALS_FILE
bq_credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_FILE)
bq_client = bigquery.Client(credentials=bq_credentials, project=PROJECT_ID)


def write_to_bigquery(df, epoch_id, target_table, write_mode=bigquery.WriteDisposition.WRITE_APPEND):
    if df.count() == 0:
        return

    pdf = df.toPandas()

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode
    )

    full_table_id = f"{PROJECT_ID}.{target_table}"
    job = bq_client.load_table_from_dataframe(pdf, full_table_id, job_config=job_config)
    job.result()
    print(f"[{target_table}] Uspešno upisano {len(pdf)} redova u BigQuery (Batch {epoch_id}).")


def process_raw_batch(df, epoch_id):
    ordered_cols = [
        "event_id", "timestamp", "station_id", "station_name", "city",
        "municipality", "latitude", "longitude", "charger_type", "energy_kwh",
        "charging_duration_minutes", "temperature_celsius", "voltage_v",
        "is_alert", "alert_reason", "processed_at"
    ]
    raw_df = df.filter(col("event_id").isNotNull()).dropDuplicates(["event_id"]).select(ordered_cols)
    write_to_bigquery(raw_df, epoch_id, RAW_TABLE_ID)
    


def process_agg_batch(df, epoch_id):
    ordered_cols = [
        "window_start", "window_end", "city", "charger_type",
        "total_energy_kwh", "avg_energy_kwh", "total_charging_sessions",
        "max_temperature_celsius", "avg_temperature_celsius", "alert_count", "calculated_at"
    ]
    agg_df = df.filter(col("window_start").isNotNull()).select(ordered_cols)
    write_to_bigquery(agg_df, epoch_id, AGG_TABLE_ID, write_mode=bigquery.WriteDisposition.WRITE_TRUNCATE)


def main():
    print("=========================================================")
    print("  SPARK STREAMING APLIKACIJA ZA OBRADU TOKA PODATAKA")
    print("=========================================================\n")

    spark = SparkSession.builder \
        .appName("EVChargingStreamProcessor") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    with open(LOOKUP_FILE, "r", encoding="utf-8") as f:
        first_line = f.readline()
        csv_delimiter = ";" if ";" in first_line else ","

    print(f"Učitavam šifarnik '{LOOKUP_FILE}' (separator: '{csv_delimiter}')...")

    stations_df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("delimiter", csv_delimiter) \
        .csv(LOOKUP_FILE)

    schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("energy_kwh", DoubleType(), True),
        StructField("charging_duration_minutes", IntegerType(), True),
        StructField("temperature_celsius", DoubleType(), True),
        StructField("voltage_v", DoubleType(), True)
    ])

    raw_stream = spark.readStream \
        .schema(schema) \
        .json(INPUT_DIR)

    cleaned_stream = raw_stream \
        .filter(col("event_id").isNotNull()) \
        .withColumn("timestamp", col("timestamp").cast(TimestampType())) \
        .withWatermark("timestamp", "10 minutes") \
        .withColumn(
            "is_alert",
            when(
                (col("temperature_celsius") > 75.0) |
                (col("voltage_v") < 360.0) | (col("voltage_v") > 430.0) |
                (col("energy_kwh") > 65.0),
                True
            ).otherwise(False)
        ) \
        .withColumn(
            "alert_reason",
            when(col("temperature_celsius") > 75.0, lit("CRITICAL_TEMPERATURE"))
            .when((col("voltage_v") < 360.0) | (col("voltage_v") > 430.0), lit("VOLTAGE_ANOMALY"))
            .when(col("energy_kwh") > 65.0, lit("POWER_SURGE"))
            .otherwise(lit("NORMAL"))
        ) \
        .withColumn("processed_at", current_timestamp())

    enriched_stream = cleaned_stream.join(
        stations_df.select("station_id", "station_name", "city", "municipality", "latitude", "longitude", "charger_type"),
        on="station_id",
        how="left"
    )

    agg_stream = enriched_stream \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("city"),
            col("charger_type")
        ) \
        .agg(
            round(_sum("energy_kwh"), 2).alias("total_energy_kwh"),
            round(avg("energy_kwh"), 2).alias("avg_energy_kwh"),
            count("event_id").alias("total_charging_sessions"),
            round(_max("temperature_celsius"), 2).alias("max_temperature_celsius"),
            round(avg("temperature_celsius"), 2).alias("avg_temperature_celsius"),
            _sum(when(col("is_alert"), 1).otherwise(0)).alias("alert_count")
        ) \
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("city"),
            col("charger_type"),
            col("total_energy_kwh"),
            col("avg_energy_kwh"),
            col("total_charging_sessions"),
            col("max_temperature_celsius"),
            col("avg_temperature_celsius"),
            col("alert_count"),
            current_timestamp().alias("calculated_at")
        )

    query_raw = enriched_stream.writeStream \
        .outputMode("append") \
        .option("checkpointLocation", "./checkpoints/raw") \
        .foreachBatch(process_raw_batch) \
        .start()

    query_agg = agg_stream.writeStream \
        .outputMode("complete") \
        .option("checkpointLocation", "./checkpoints/agg") \
        .foreachBatch(process_agg_batch) \
        .start()

    print("\n Spark Streaming je aktivan sa Watermark-om (10 min) i novim pravilima anomalija!")
    
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()