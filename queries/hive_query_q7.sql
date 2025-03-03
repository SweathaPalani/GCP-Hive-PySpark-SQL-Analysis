SELECT  
    d.department,  
    COUNT(CASE  
        WHEN o.order_hour_of_day < 12 THEN op.order_id  
    END) AS morning_count,  
    COUNT(CASE  
        WHEN o.order_hour_of_day >= 17 THEN op.order_id  
    END) AS evening_count  
FROM  
    orders o  
JOIN  
    order_products op ON o.order_id = op.order_id  
JOIN  
    products p ON op.product_id = p.product_id  
JOIN  
    departments d ON p.department_id = d.department_id  
GROUP BY  
    d.department  
ORDER BY  
    d.department 
LIMIT 100;
