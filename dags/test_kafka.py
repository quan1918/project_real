from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaTest")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .getOrCreate()
)

df = (
    spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "airbnb_topic")
        .load()
)

df.printSchema()
