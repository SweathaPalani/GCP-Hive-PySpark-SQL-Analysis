# Question 2: What is the mean and the variance of number of products in an aisle?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, expr

spark = SparkSession.builder.appName("Product Counts Analysis").getOrCreate()

aisles_df = spark.read.csv("gs://dataproc-staging-us-central1-279010307144\
iimvky2x/retail-header/aisles.csv", header=True, inferSchema=True)

products_df = spark.read.csv("gs://dataproc-staging-us-central1-279010307144\
iimvky2x/retail-header/products.csv", header=True, inferSchema=True)

aisles_df.createOrReplaceTempView("aisles")
products_df.createOrReplaceTempView("products")

product_counts_df = spark.sql("""
    SELECT a.aisle_id, COUNT(p.product_id) AS num_products
    FROM aisles a
    JOIN products p ON a.aisle_id = p.aisle_id
    GROUP BY a.aisle_id
""")

mean_products = product_counts_df.agg(avg("num_products").alias("mean_products_per_aisle")).first()[0]

variance_products = product_counts_df.agg(
    sum((col("num_products") - mean_products) ** 2).alias("variance_sum")
).first()[0] / (product_counts_df.count() - 1)

print(f"Mean Products per Aisle: {mean_products}")
print(f"Variance Products per Aisle: {variance_products}")

spark.stop()