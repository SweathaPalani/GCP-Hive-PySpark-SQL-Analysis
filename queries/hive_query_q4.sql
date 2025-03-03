-- Create temporary tables for bakery and frozen products
CREATE TEMPORARY TABLE bakery_products AS
SELECT products.product_id
FROM products
JOIN departments ON products.department_id = departments.department_id
WHERE departments.department = 'bakery';

CREATE TEMPORARY TABLE frozen_products AS
SELECT products.product_id
FROM products
JOIN departments ON products.department_id = departments.department_id
WHERE departments.department = 'frozen';

-- Get total number of orders and orders with both bakery and frozen products
SELECT 
    (SELECT COUNT(DISTINCT order_products.order_id) FROM order_products) AS total_orders,
    (SELECT COUNT(DISTINCT order_products1.order_id) 
    FROM order_products order_products1 
    JOIN bakery_products ON order_products1.product_id = bakery_products.product_id
    JOIN order_products order_products2 ON order_products1.order_id = order_products2.order_id
    JOIN frozen_products ON order_products2.product_id = frozen_products.product_id) AS orders_with_both;
