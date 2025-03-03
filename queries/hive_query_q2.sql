CREATE TEMPORARY TABLE product_counts AS
SELECT  
    a.aisle_id,  
    a.aisle,  
    COUNT(p.product_id) AS num_products
FROM aisles a
JOIN products p  
ON a.aisle_id = p.aisle_id
GROUP BY a.aisle_id, a.aisle;

SELECT  
    AVG(num_products) AS mean_products_per_aisle
FROM product_counts;

SELECT  
    AVG(num_products) AS mean_products_per_aisle,
    SUM((num_products - avg_count.mean_products_per_aisle) *  
    (num_products - avg_count.mean_products_per_aisle)) /  
    (COUNT(*) - 1) AS variance_products_per_aisle
FROM product_counts,  
(SELECT AVG(num_products) AS mean_products_per_aisle FROM product_counts) avg_count;