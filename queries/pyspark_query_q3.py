# Question 3: Which aisle products are most bought with products from the "speciality cheeses" aisle?

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("RetailDataAnalysis").getOrCreate()

# Define the GCP bucket path
bucket_path = 'gs://dataproc-staging-us-central1-35264236635-xgzimmzm/retail'
order_products = 'gs://dataproc-staging-us-central1-35264236635-xgzimmzm/retail/order_products/'
products = 'gs://dataproc-staging-us-central1-35264236635-xgzimmzm/retail/products/'
aisles = 'gs://dataproc-staging-us-central1-35264236635-xgzimmzm/retail/aisles/'

# Load data from GCP bucket (adjust paths accordingly)
order_products_df = spark.read.csv(f'{order_products}/order_products.csv', header=True, inferSchema=True)
products_df = spark.read.csv(f'{products}/products.csv', header=True, inferSchema=True)
aisles_df = spark.read.csv(f'{aisles}/aisles.csv', header=True, inferSchema=True)

# Create a DataFrame for cheese orders
cheese_orders_df = (
    order_products_df
    .join(products_df, "product_id")
    .join(aisles_df, "aisle_id")
    .filter(col("aisle") == "specialty cheeses")
    .select("order_id").distinct()
)

# Find the aisle with the most orders that are not 'specialty cheeses'
result_df = (
    order_products_df
    .join(products_df, "product_id")
    .join(aisles_df, "aisle_id")
    .join(cheese_orders_df, "order_id")
    .filter(col("aisle") != "specialty cheeses")
    .groupBy("aisle")
    .agg(countDistinct("order_id").alias("count"))
    .orderBy(col("count").desc())
    .limit(1)
)

# Show the result
result_df.show()
