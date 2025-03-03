CREATE EXTERNAL TABLE orders (
  order_id int,
  user_id int,
  eval_set string,
  order_number int,
  order_dow int,
  order_hour_of_day int,
  days_since_prior_order int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/orders/';

LOAD DATA INPATH 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/orders.csv' 
INTO TABLE orders;

CREATE EXTERNAL TABLE order_products (
  order_id int,
  product_id int,
  add_to_cart_order int,
  reordered int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/order_products/';

LOAD DATA INPATH 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/order_products.csv' 
INTO TABLE order_products;

CREATE EXTERNAL TABLE departments (
  department_id int,
  department string
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/departments/';

LOAD DATA INPATH 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/departments.csv' 
INTO TABLE departments;

CREATE EXTERNAL TABLE products (
  product_id int,
  product_name string,
  aisle_id int,
  department_id int
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/products/';

LOAD DATA INPATH 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/products.csv' 
INTO TABLE products;

CREATE EXTERNAL TABLE aisles ( 
  aisle_id int,
  aisle varchar(300)
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/aisles/';

LOAD DATA INPATH 'gs://dataproc-staging-us-central1-759092076344-putmmqsc/retail/aisles.csv' 
INTO TABLE aisles;