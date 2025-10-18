import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, avg
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.ml import PipelineModel
import psycopg2
from psycopg2.extras import execute_values


# === Config via env ===
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
POSTGRES_URL = os.getenv("POSTGRES_URL", "jdbc:postgresql://postgres:5432/restaurant_db")
POSTGRES_USER = os.getenv("PGUSER", "restaurant")
POSTGRES_PASSWORD = os.getenv("PGPASSWORD", "restaurant")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/opt/airflow/checkpoints/orders_weather")
MODEL_PATH = os.getenv("MODEL_PATH", "/opt/models/revenue_model")

# === Spark session ===
spark = SparkSession.builder.appName("RestaurantStreaming").config("spark.sql.shuffle.partitions", "2").getOrCreate()

# === Schemas ===
order_schema = StructType().add("timestamp", StringType()) \
    .add("branch_id", StringType()) \
    .add("revenue", DoubleType()) \
    .add("service_time", DoubleType()) \
    .add("rating", DoubleType())

weather_schema = StructType().add("timestamp", StringType()) \
    .add("city", StringType()) \
    .add("temp", DoubleType()) \
    .add("weather", StringType())

# === Read from Kafka (multi-topic) ===
raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers",KAFKA_BOOTSTRAP) \
    .option("subscribe", "orders,weather") \
    .option("startingOffsets", "latest") \
    .load()

val = raw.selectExpr("topic", "CAST(value AS STRING) as json_str", "timestamp as kafka_ts")

orders_df = val.filter(col("topic") == "orders").select(from_json(col("json_str"), order_schema).alias("d")) \
    .selectExpr("d.timestamp as timestamp", "d.branch_id", "d.revenue", "d.service_time", "d.rating") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

weather_df = val.filter(col("topic") == "weather") \
    .select(from_json(col("json_str"), weather_schema).alias("d")) \
    .selectExpr("d.timestamp as timestamp", "d.city", "d.temp", "d.weather") \
    .withColumn("timestamp", to_timestamp(col("timestamp")))

# === Windowed aggregation (5-minute tumbling window) ===
orders_agg = orders_df.withWatermark("timestamp", "2 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes"), col("branch_id")) \
    .agg(
        avg("revenue").alias("avg_revenue"),
        avg("service_time").alias("avg_service_time"),
        avg("rating").alias("avg_rating")
    ) \
    .selectExpr("window.start as window_start", "window.end as window_end", "branch_id", "avg_revenue", "avg_service_time", "avg_rating")

# === Upsert to Postgres in foreachBatch (idempotent) ===
def upsert_kpi(batch_df, batch_id):
    # run on driver, so safe to use psycopg2
    import psycopg2
    from psycopg2.extras import execute_batch

    if batch_df.rdd.isEmpty():
        return
    
    pdf = batch_df \
        .withColumn("window_start", col("window_start").cast("string")) \
        .withColumn("window_end", col("window_end").cast("string")) \
        .toPandas()

    if pdf.empty:
        return
    
    tuples = [tuple(x) for x in pdf[['window_start', 'window_end', 'branch_id', 'avg_revenue', 'avg_service_time', 'avg_rating']].to_numpy()]

    conn = psycopg2.connect(dbname='restaurant_db', user='airflow', password='airflow', host='postgres', port=5432)
    cur = conn.cursor()
    sql = """
    INSERT INTO kpi_5min (window_start, window_end, branch_id, avg_revenue, avg_service_time, avg_rating)
    VALUES %s
    ON CONFLICT (window_start, branch_id) DO UPDATE
    SET window_end = EXCLUDED.window_end,
        avg_revenue = EXCLUDED.avg_revenue,
        avg_service_time = EXCLUDED.avg_service_time,
        avg_rating = EXCLUDED.avg_rating;
    """
    execute_values(cur, sql, tuples)
    conn.commit()
    cur.close()
    conn.close()

query_kpi = orders_agg.writeStream.outputMode("update") \
    .foreachBatch(upsert_kpi) \
    .option("checkpointLocation", CHECKPOINT_DIR + "/kpi") \
    .start()

# === Optional: load ML model and predict on streaming orders ===
pred_query = None
if os.path.exists(MODEL_PATH):
    try:
        model = PipelineModel.load(MODEL_PATH)
        # model must expect features available in order_df
        preds = model.transform(orders_df) #adds column "prediction" typically
        # select cols to write
        preds_out = preds.select(col("timestamp").alias("ts"),"branch_id","revenue", col("prediction").alias("predicted_revenue"))
        # write preds to Postgres using foreachBatch similarly
        def write_preds(batch_df, batch_id):
            import psycopg2
            from psycopg2.extras import execute_values
            if batch_df.rdd.isEmpty():
                return
            pdf = batch_df.toPandas()
            tuples  = [tuple(x) for x in pdf[['ts', 'branch_id','revenue', 'predicted_revenue']].to_numpy()]
            conn = psycopg2.connect(dbname='restaurant_db', user='airflow', password='airflow', host='postgres', port=5432)
            cur = conn.cursor()
            sql = "INSERT INTO predictions (ts, branch_id, revenue, predicted_revenue) VALUES %s"
            execute_values(cur, sql, tuples)
            conn.commit()
            cur.close()
            conn.close()
        pred_query = preds_out.writeStream.foreachBatch(write_preds).option("checkpointLocation", CHECKPOINT_DIR + "/pred").start()
    except Exception as e:
        print("Failed to load/apply model:", e)
else:
    print("Model not found at", MODEL_PATH)

 # Wait for termination
if pred_query:
    pred_query.awaitTermination()
query_kpi.awaitTermination()
    