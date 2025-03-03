WITH top_user_orders AS (
  SELECT COUNT(*) AS orders, user_id, order_dow 
  FROM orders 
  GROUP BY user_id, order_dow
) 
SELECT d.department, o.order_dow, 
       COUNT(DISTINCT op.product_id) AS num_products, 
       COUNT(DISTINCT o.order_id) AS num_orders, 
       COUNT(*) AS num_items
FROM top_user_orders tuo 
LEFT JOIN orders o ON tuo.user_id = o.user_id
LEFT JOIN order_products op ON op.order_id = o.order_id
LEFT JOIN products p ON p.product_id = op.product_id
LEFT JOIN departments d ON p.department_id = d.department_id
GROUP BY d.department, o.order_dow
ORDER BY num_orders DESC
LIMIT 50;