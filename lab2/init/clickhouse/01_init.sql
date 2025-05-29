CREATE DATABASE IF NOT EXISTS bigdata;

USE bigdata;

-- 1. Витрина продаж по продуктам
CREATE TABLE product_sales_mart (
    product_id UInt32,
    product_name String,
    category_name String,
    total_quantity_sold UInt32,
    total_revenue Float64,
    avg_rating Float64,
    total_reviews UInt32
) ENGINE = MergeTree()
ORDER BY (category_name, product_id);

-- 2. Витрина продаж по клиентам
CREATE TABLE customer_sales_mart (
    customer_id UInt32,
    first_name String,
    last_name String,
    country_name String,
    total_purchase Float64,
    average_check Float64
) ENGINE = MergeTree()
ORDER BY (country_name, customer_id);

-- 3. Витрина продаж по времени
CREATE TABLE time_sales_mart (
    year UInt16,
    month UInt8,
    total_sales UInt32,
    total_revenue Float64,
    avg_order_value Float64
) ENGINE = MergeTree()
ORDER BY (year, month);

-- 4. Витрина продаж по магазинам
CREATE TABLE store_sales_mart (
    store_id UInt32,
    store_name String,
    city String,
    country_name String,
    total_revenue Float64,
    average_check Float64
) ENGINE = MergeTree()
ORDER BY (country_name, city, store_id);

-- 5. Витрина продаж по поставщикам
CREATE TABLE supplier_sales_mart (
    supplier_id UInt32,
    supplier_name String,
    country_name String,
    total_revenue Float64,
    avg_product_price Float64
) ENGINE = MergeTree()
ORDER BY (country_name, supplier_id);

-- 6. Витрина качества продукции
CREATE TABLE product_quality_mart (
    product_id UInt32,
    product_name String,
    avg_rating Float64,
    total_reviews UInt32,
    total_quantity_sold UInt32
) ENGINE = MergeTree()
ORDER BY product_id;