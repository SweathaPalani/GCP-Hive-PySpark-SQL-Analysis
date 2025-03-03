---------------------------------------------------------------------------------------
-- 1. Joining tables Product & Order_Products by Product_Id
-- 2. Calculates total reorders & order of each product
-- 3. Lists top 3 of the records in desc order or reorder/order
---------------------------------------------------------------------------------------
SELECT COUNT(order_id) AS order_sum,  
SUM(reordered) AS reorder_sum,  
product_name  
FROM order_products op  
JOIN products p  
ON op.product_id = p.product_id  
GROUP BY p.product_name 
HAVING reorder_sum > 500 
ORDER BY reorder_sum DESC, order_sum DESC 
LIMIT 3;
