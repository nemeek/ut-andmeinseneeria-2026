SELECT COUNT(*) AS kaupade_arv
FROM messy;

SELECT product_name, category, unit_price_eur
FROM messy    
ORDER BY unit_price_eur DESC
LIMIT 5;

SELECT category, COUNT(*) AS kaupu
FROM messy
GROUP BY category
ORDER BY kaupu DESC;

