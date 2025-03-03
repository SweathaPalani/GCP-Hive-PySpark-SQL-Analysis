# Question 5: Name the top 5 products which are the most reordered 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, count

# Initialize Spark session
spark = SparkSession.builder.appName("TopReorderedProductsUnique").getOrCreate()

# Bucket path where files are stored
bucket_path = "gs://dataproc-staging-us-central1-60905145912m5qjuknd/pySpark/"

# Load datasets
order_products = spark.read.csv(f"{bucket_path}order_products.csv", header=True, inferSchema=True)
products = spark.read.csv(f"{bucket_path}products.csv", header=True, inferSchema=True)

# Filter for reordered products and join with products table
reordered_products = order_products.filter(col("reordered") == 1)\
    .select("product_id", "order_id")\
    .join(products.select("product_id", "product_name"), on="product_id", how="inner")

# Group by product name, count reorders
top_reordered_products = reordered_products.groupBy("product_name")\
    .agg(count("product_id").alias("reorder_count"))\
    .orderBy(desc("reorder_count"))\
    .limit(5)

# Show the top 5 most reordered products
top_reordered_products.show(truncate=False)

# Stop the Spark session
spark.stop()