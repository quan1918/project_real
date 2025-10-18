from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml import Pipeline

spark = SparkSession.builder.appName("TrainLR").getOrCreate()
df = spark.read.csv('/opt/airflow/data/historical_orders.csv', header=True, inferSchema=True)

assembler = VectorAssembler(inputCols=["service_time", "rating"], outputCol="feature")
lr = LinearRegression(featuresCol="feature", labelCol="revenue")
pipeline = Pipeline(stages=[assembler, lr])

model = pipeline.fit(df)
model.write().overwrite().save("revenue_model")
print("Save model to revenue_model")
