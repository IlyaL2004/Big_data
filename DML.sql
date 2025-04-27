INSERT INTO dim_country (country_name)
SELECT DISTINCT country FROM (
    SELECT customer_country AS country FROM mock_data_raw
    UNION ALL
    SELECT seller_country FROM mock_data_raw
    UNION ALL
    SELECT store_country FROM mock_data_raw
    UNION ALL
    SELECT supplier_country FROM mock_data_raw
) AS countries
WHERE country IS NOT NULL;

-- Типы питомцев
INSERT INTO dim_pet_type (type_name)
SELECT DISTINCT customer_pet_type
FROM mock_data_raw
WHERE customer_pet_type IS NOT NULL;

-- Породы питомцев
INSERT INTO dim_pet_breed (breed_name)
SELECT DISTINCT customer_pet_breed
FROM mock_data_raw
WHERE customer_pet_breed IS NOT NULL;

-- Категории питомцев
INSERT INTO dim_pet_category (category_name)
SELECT DISTINCT pet_category
FROM mock_data_raw
WHERE pet_category IS NOT NULL;

-- Категории продуктов
INSERT INTO dim_product_category (name)
SELECT DISTINCT product_category
FROM mock_data_raw
WHERE product_category IS NOT NULL;

-- Бренды
INSERT INTO dim_brand (name)
SELECT DISTINCT product_brand
FROM mock_data_raw
WHERE product_brand IS NOT NULL;

-- Материалы
INSERT INTO dim_material (name)
SELECT DISTINCT product_material
FROM mock_data_raw
WHERE product_material IS NOT NULL;

-- Цвета
INSERT INTO dim_color (name)
SELECT DISTINCT product_color
FROM mock_data_raw
WHERE product_color IS NOT NULL;

-- Размеры
INSERT INTO dim_size (name)
SELECT DISTINCT product_size
FROM mock_data_raw
WHERE product_size IS NOT NULL;

----------------

-- Клиенты
INSERT INTO dim_customer (
    first_name,
    last_name,
    email,
    age,
    postal_code,
    country_id
)
SELECT
    r.customer_first_name,
    r.customer_last_name,
    r.customer_email,
    r.customer_age,
    r.customer_postal_code,
    c.country_id
FROM mock_data_raw r
JOIN dim_country c ON r.customer_country = c.country_name
WHERE r.customer_email IS NOT NULL
GROUP BY
    r.customer_first_name,
    r.customer_last_name,
    r.customer_email,
    r.customer_age,
    r.customer_postal_code,
    c.country_id;

-- Продавцы
INSERT INTO dim_seller (
    first_name,
    last_name,
    email,
    postal_code,
    country_id
)
SELECT
    r.seller_first_name,
    r.seller_last_name,
    r.seller_email,
    r.seller_postal_code,
    c.country_id
FROM mock_data_raw r
JOIN dim_country c ON r.seller_country = c.country_name
WHERE r.seller_email IS NOT NULL
GROUP BY
    r.seller_first_name,
    r.seller_last_name,
    r.seller_email,
    r.seller_postal_code,
    c.country_id;

-- Магазины
INSERT INTO dim_store (
    name,
    location,
    city,
    state,
    country_id,
    phone,
    email
)
SELECT
    r.store_name,
    r.store_location,
    r.store_city,
    r.store_state,
    c.country_id,
    r.store_phone,
    r.store_email
FROM mock_data_raw r
JOIN dim_country c ON r.store_country = c.country_name
WHERE r.store_name IS NOT NULL
GROUP BY
    r.store_name,
    r.store_location,
    r.store_city,
    r.store_state,
    c.country_id,
    r.store_phone,
    r.store_email;

-- Поставщики
INSERT INTO dim_supplier (
    name,
    contact_name,
    email,
    phone,
    address,
    city,
    country_id
)
SELECT
    r.supplier_name,
    r.supplier_contact,
    r.supplier_email,
    r.supplier_phone,
    r.supplier_address,
    r.supplier_city,
    c.country_id
FROM mock_data_raw r
JOIN dim_country c ON r.supplier_country = c.country_name
WHERE r.supplier_name IS NOT NULL
GROUP BY
    r.supplier_name,
    r.supplier_contact,
    r.supplier_email,
    r.supplier_phone,
    r.supplier_address,
    r.supplier_city,
    c.country_id;

-- Продукты
INSERT INTO dim_product (
    name,
    category_id,
    brand_id,
    material_id,
    color_id,
    size_id,
    weight,
    description,
    rating,
    reviews,
    release_date,
    expiry_date
)
SELECT
    r.product_name,
    pc.category_id,
    b.brand_id,
    m.material_id,
    col.color_id,
    s.size_id,
    r.product_weight,
    r.product_description,
    r.product_rating,
    r.product_reviews,
    TO_DATE(r.product_release_date, 'MM/DD/YYYY'),
    TO_DATE(r.product_expiry_date, 'MM/DD/YYYY')
FROM mock_data_raw r
LEFT JOIN dim_product_category pc ON r.product_category = pc.name
LEFT JOIN dim_brand b ON r.product_brand = b.name
LEFT JOIN dim_material m ON r.product_material = m.name
LEFT JOIN dim_color col ON r.product_color = col.name
LEFT JOIN dim_size s ON r.product_size = s.name
WHERE r.product_name IS NOT NULL
GROUP BY
    r.product_name,
    pc.category_id,
    b.brand_id,
    m.material_id,
    col.color_id,
    s.size_id,
    r.product_weight,
    r.product_description,
    r.product_rating,
    r.product_reviews,
    r.product_release_date,
    r.product_expiry_date;


-- Заполнение таблицы дат
INSERT INTO dim_date (date, year, month, day, weekday)
SELECT
    sale_date::DATE,
    EXTRACT(YEAR FROM sale_date::DATE),
    EXTRACT(MONTH FROM sale_date::DATE),
    EXTRACT(DAY FROM sale_date::DATE),
    TO_CHAR(sale_date::DATE, 'Day')
FROM (
    SELECT DISTINCT sale_date
    FROM mock_data_raw
    WHERE sale_date IS NOT NULL
) AS dates
ON CONFLICT (date) DO NOTHING;


-- Факты продаж

INSERT INTO sales_fact (
    sale_date,
    customer_id,
    seller_id,
    product_id,
    store_id,
    supplier_id,
    quantity,
    total_price
)
SELECT
    r.sale_date::DATE,
    (SELECT customer_id FROM dim_customer WHERE email = r.customer_email),
    (SELECT seller_id FROM dim_seller WHERE email = r.seller_email),
    (SELECT product_id FROM dim_product
     WHERE name = r.product_name
       AND description = r.product_description
       AND release_date = TO_DATE(r.product_release_date, 'MM/DD/YYYY')
       AND rating = r.product_rating
       AND reviews = r.product_reviews::integer),
    (SELECT store_id FROM dim_store WHERE email = r.store_email),
    (SELECT supplier_id FROM dim_supplier WHERE email = r.supplier_email),
    r.sale_quantity,
    r.sale_total_price
FROM mock_data_raw r
