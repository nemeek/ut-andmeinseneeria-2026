DROP TABLE IF EXISTS messy; 
CREATE TABLE IF NOT EXISTS messy (
    order_id INTEGER PRIMARY KEY,
    order_date DATE,
    customer_email TEXT NOT NULL,
    country TEXT NOT NULL,
    product_sku TEXT NOT NULL,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    quantity INTEGER,
    unit_price_eur MONEY,
    status TEXT
);
