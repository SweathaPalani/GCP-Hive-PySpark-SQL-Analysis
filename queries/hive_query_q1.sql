SELECT aisles.aisle, COUNT(*) AS total_orders
FROM order_products
JOIN products ON order_products.product_id = products.product_id
JOIN aisles ON products.aisle_id = aisles.aisle_id
GROUP BY aisles.aisle
ORDER BY total_orders DESC
LIMIT 3;