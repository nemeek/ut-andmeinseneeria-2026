TRUNCATE TABLE messy;

COPY messy (order_id,order_date,customer_email,country,product_sku,product_name,category,quantity,unit_price_eur,status)
FROM '/data/orders_messy_semicolon.csv'
WITH (
    FORMAT csv,
    HEADER true,
    DELIMITER ';',
    ENCODING 'UTF8'
);
