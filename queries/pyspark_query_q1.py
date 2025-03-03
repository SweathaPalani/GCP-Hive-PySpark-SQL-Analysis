# Question 1: Name the top 3 aisles with the most products ordered 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder.appName("Top Aisles").getOrCreate()

# Load datasets from Google Cloud Storage
order_products = spark.read.csv(
    "gs://dataproc-staging-us-central1-611682968748dftv2pyf/order_products/order_products.csv",
    inferSchema=True,
    header=True
)
products = spark.read.csv(
    "gs://dataproc-staging-us-central1-611682968748dftv2pyf/retail/products/products.csv",
    inferSchema=True,
    header=True
)
aisles = spark.read.csv(
    "gs://dataproc-staging-us-central1-611682968748dftv2pyf/retail/aisles/aisles.csv",
    inferSchema=True,
    header=True
)

# Join datasets
joined_data = (
    order_products.join(products, "product_id")
    .join(aisles, "aisle_id")
)

# Aggregate total orders per aisle and get the top 3
result = (
    joined_data.groupBy("aisle")
    .agg(count("*").alias("total_orders"))
    .orderBy(col("total_orders").desc())
    .limit(3)
)

# Show results
result.show()

# Stop Spark session
spark.stop()