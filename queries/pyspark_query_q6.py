''' Question 6: Which 3 products should a customer retargeting (to bring customers back) campaign offer 
discounts on? 
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum
import subprocess

spark = SparkSession.builder.appName("OrdersAnalysis").getOrCreate()

# Function to check file exists or not
def gcs_file_exists(gcs_path):
    try:
        result = subprocess.run(['gsutil', 'ls', gcs_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return result.returncode == 0  # Return True if the file exists
    except Exception as e:
        print(f"Error checking file {gcs_path}: {e}")
        return False

# Initializing File paths
order_products_path = "gs://dataproc-staging-us-central1-202552022924wn57btbb/retail/dataFile_Reupload/order_products.csv"
products_path = "gs://dataproc-staging-us-central1-202552022924wn57btbb/retail/dataFile_Reupload/products.csv"
departments_path = "gs://dataproc-staging-us-central1-202552022924wn57btbb/retail/dataFile_Reupload/departments.csv"

# Check if files exist or not
if not gcs_file_exists(order_products_path):
    raise FileNotFoundError(f"The file {order_products_path} does not exist.")
if not gcs_file_exists(products_path):
    raise FileNotFoundError(f"The file {products_path} does not exist.")
if not gcs_file_exists(departments_path):
    raise FileNotFoundError(f"The file {departments_path} does not exist.")

# Loading data
order_products = spark.read.format("csv").option("header","true").load(order_products_path)
products = spark.read.format("csv").option("header","true").load(products_path)
departments = spark.read.format("csv").option("header","true").load(departments_path)

# Joining the tables order_products and products using product_id
joined_df = order_products.join(products, order_products["product_id"] == products["product_id"])

# Applying the various conditions on the dataset
aggregated_df = joined_df.groupBy("product_name")\
    .agg(
        count("order_id").alias("order_sum"),
        sum("reordered").alias("reorder_sum")
    )

filtered_df = aggregated_df.filter(col("reorder_sum") > 500)

# Setting the final records ordering and applying the limits on number of results
result_df = filtered_df.orderBy(col("reorder_sum").desc(), col("order_sum").desc()).limit(3)

# Displaying the results
result_df.show()