WITH cheese_orders AS (
    SELECT DISTINCT op.order_id
    FROM order_products op
    JOIN products p ON op.product_id = p.product_id
    JOIN aisles a ON p.aisle_id = a.aisle_id
    WHERE a.aisle = 'specialty cheeses'
)
SELECT a.aisle, COUNT(*) AS count
FROM order_products op
JOIN products p ON op.product_id = p.product_id
JOIN aisles a ON p.aisle_id = a.aisle_id
WHERE op.order_id IN (SELECT order_id FROM cheese_orders)
JOIN a.aisle != 'specialty cheeses'
GROUP BY a.aisle
ORDER BY count DESC
LIMIT 1;