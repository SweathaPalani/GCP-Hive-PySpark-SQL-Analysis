'''Question 8: 8. What is the average number of products in an order? What is the max?'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType
from pyspark.sql.functions import count, avg

# Initialize Spark session
spark = SparkSession.builder.appName("ProductOrderAnalysis").getOrCreate()

# Define schema
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True)
])

# Load the CSV file into a DataFrame without header
order_products_df = spark.read.csv("gs://dataproc-staging-us-central1-653692749797jcwrczz0/retail/order_products/order_products.csv", schema=schema, header=False)

# Analysis 1: Maximum number of products in any order
max_products_order_df = (order_products_df.groupBy("order_id")
    .agg(count("product_id").alias("Max_products"))
    .orderBy("Max_products", ascending=False)
    .limit(1))

print("Maximum number of products in any order:")
max_products_order_df.show()

# Analysis 2: Average number of products per order
order_df = order_products_df.groupBy("order_id").agg(count("product_id").alias("total_products"))
avg_no_of_products = order_df.agg(avg("total_products").alias("AVG_no_of_products")).collect()[0][0]

# Result
print(f"Average number of products per order: {avg_no_of_products}")