SELECT p.product_name, COUNT(op.product_id) AS reorder_count  
FROM order_products op  
JOIN products p ON op.product_id = p.product_id  
WHERE op.reordered = '1'    
GROUP BY p.product_name  
ORDER BY reorder_count DESC  
LIMIT 5;

SELECT p.product_id, p.product_name,
COUNT(*) AS count_order 
FROM order_products
LEFT JOIN products p ON p.product_id = order_products.product_id
WHERE order_products.reordered=1
GROUP BY p.product_id
ORDER BY count_order DESC
LIMIT 5;