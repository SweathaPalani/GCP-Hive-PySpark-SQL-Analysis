"""
Question 4: 4. Sales department is making a recommendation that frozen products should be placed next 
to bakery products. Write 1-2 SQL queries to get the data that would support/oppose this recommendation.
"""
from pyspark.sql i

from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct

# Initialize Spark session
spark = SparkSession.builder.appName("OrdersAnalysis").getOrCreate()

# Load data into DataFrames
order_products = spark.read.format("csv").option("header","true").load("gs://dataproc-staging-us-central1-854851855982-3tw884wv/data/order_products.csv")
products = spark.read.format("csv").option("header","true").load("gs://dataproc-staging-us-central1-854851855982-3tw884wv/data/products.csv")
departments = spark.read.format("csv").option("header","true").load("gs://dataproc-staging-us-central1-854851855982-3tw884wv/data/departments.csv")

# Filter bakery and frozen products
bakery_products = products.join(departments, products.department_id == departments.department_id)\
                          .filter(departments.department == 'bakery')\
                          .select("product_id")

frozen_products = products.join(departments, products.department_id == departments.department_id)\
                          .filter(departments.department == 'frozen')\
                          .select("product_id")

# Get orders with bakery and frozen products
orders_with_bakery = order_products.join(bakery_products,"product_id").select("order_id").distinct()
orders_with_frozen = order_products.join(frozen_products,"product_id").select("order_id").distinct()

# Get total orders and orders with both bakery and frozen products
total_orders = order_products.select("order_id").distinct().count()
orders_with_both = orders_with_bakery.join(orders_with_frozen,"order_id").count()

print(f"Total Orders: {total_orders}")
print(f"Orders with both bakery and frozen products: {orders_with_both}")