'''
Question 7: Do orders from different departments vary over time of the day? Are there morning and 
evening departments (popular in the morning and popular in the evening)? 
'''

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import col, count, when

# Initialize Spark session
spark = SparkSession.builder.appName("OrderTimeOfDay").getOrCreate()

# Define schema for the 'orders' table
orders_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("eval_set", StringType(), True),
    StructField("order_number", IntegerType(), True),
    StructField("order_dow", IntegerType(), True),
    StructField("order_hour_of_day", IntegerType(), True),
    StructField("days_since_prior_order", IntegerType(), True)
])

# Define schema for the 'order_products' table
order_products_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True)
])

# Define schema for the 'departments' table
departments_schema = StructType([
    StructField("department_id", IntegerType(), True),
    StructField("department", StringType(), True)
])

# Define schema for the 'products' table
products_schema = StructType([
    StructField("product_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("aisle_id", IntegerType(), True),
    StructField("department_id", IntegerType(), True)
])

# Load the CSV files (or other formats), assuming you don't have headers
orders = spark.read.csv("path_to_orders.csv", schema=orders_schema, header=False)
order_products = spark.read.csv("path_to_order_products.csv", schema=order_products_schema, header=False)
departments = spark.read.csv("path_to_departments.csv", schema=departments_schema, header=False)
products = spark.read.csv("path_to_products.csv", schema=products_schema, header=False)

# Perform JOIN operations between orders, order_products, products, and departments
order_department = orders.join(order_products, "order_id")\
    .join(products, "product_id")\
    .join(departments, "department_id")

# Calculate morning and evening order counts
order_time_of_day_count = order_department.groupBy("department")\
    .agg(
        count(when(col("order_hour_of_day") < 12, col("order_id"))).alias("morning_count"),
        count(when(col("order_hour_of_day") >= 17, col("order_id"))).alias("evening_count")
    )\
    .orderBy("department")\
    .limit(100)

# Show the result
order_time_of_day_count.show()