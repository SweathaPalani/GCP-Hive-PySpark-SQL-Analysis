WITH order_counts AS ( 
    SELECT  
        order_id, 
        COUNT(product_id) AS product_count 
    FROM order_products 
    GROUP BY order_id 
) 
SELECT  
    AVG(product_count) AS avg_products_per_order, 
    MAX(product_count) AS max_products_per_order 
FROM order_counts;