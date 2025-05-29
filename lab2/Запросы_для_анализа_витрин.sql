-- 1.1 Топ-10 самых продаваемых продуктов
SELECT
    product_name,
    total_quantity_sold
FROM bigdata.product_sales_mart
ORDER BY total_quantity_sold DESC
LIMIT 10;

-- 1.2 Общая выручка по категориям продуктов
SELECT
    category_name,
    SUM(total_revenue) AS category_revenue
FROM bigdata.product_sales_mart
GROUP BY category_name
ORDER BY category_revenue DESC;

-- 1.3 Средний рейтинг и количество отзывов для каждого продукта
SELECT
    product_name,
    avg_rating,
    total_reviews
FROM bigdata.product_sales_mart
ORDER BY product_id;

-- 2.1 Топ-10 клиентов с наибольшей общей суммой покупок
SELECT
    first_name,
    last_name,
    total_purchase
FROM bigdata.customer_sales_mart
ORDER BY total_purchase DESC
LIMIT 10;

-- 2.2 Распределение клиентов по странам
SELECT
    country_name,
    COUNT(*) AS customer_count
FROM bigdata.customer_sales_mart
GROUP BY country_name
ORDER BY customer_count DESC;

-- 2.3 Средний чек для каждого клиента
SELECT
    first_name,
    last_name,
    average_check
FROM bigdata.customer_sales_mart
ORDER BY customer_id;


-- 3.1 Месячные и годовые тренды продаж
SELECT
    year,
    month,
    total_sales,
    total_revenue
FROM bigdata.time_sales_mart
ORDER BY year, month;

-- 3.2 Сравнение выручки за разные периоды
-- Годовое сравнение
SELECT
    year,
    SUM(total_revenue) AS year_revenue
FROM bigdata.time_sales_mart
GROUP BY year
ORDER BY year;

-- Квартальное сравнение
SELECT
    year,
    toQuarter(toDate(concat(toString(year), '-', toString(month), '-01'))) AS quarter,
    SUM(total_revenue) AS quarter_revenue
FROM bigdata.time_sales_mart
GROUP BY year, quarter
ORDER BY year, quarter;

-- 3.3 Средний размер заказа по месяцам
SELECT
    year,
    month,
    avg_order_value
FROM bigdata.time_sales_mart
ORDER BY year, month;


-- 4.1 Топ-5 магазинов с наибольшей выручкой
SELECT
    store_name,
    total_revenue
FROM bigdata.store_sales_mart
ORDER BY total_revenue DESC
LIMIT 5;

-- 4.2 Распределение продаж по городам и странам
-- По странам
SELECT
    country_name,
    SUM(total_revenue) AS country_revenue
FROM bigdata.store_sales_mart
GROUP BY country_name
ORDER BY country_revenue DESC;

-- По городам
SELECT
    city,
    SUM(total_revenue) AS city_revenue
FROM bigdata.store_sales_mart
GROUP BY city
ORDER BY city_revenue DESC;

-- 4.3 Средний чек для каждого магазина
SELECT
    store_name,
    average_check
FROM bigdata.store_sales_mart
ORDER BY store_id;

-- 5.1 Топ-5 поставщиков с наибольшей выручкой
SELECT
    supplier_name,
    total_revenue
FROM bigdata.supplier_sales_mart
ORDER BY total_revenue DESC
LIMIT 5;

-- 5.2 Средняя цена товаров от каждого поставщика
SELECT
    supplier_name,
    avg_product_price
FROM bigdata.supplier_sales_mart
ORDER BY supplier_id;

-- 5.3 Распределение продаж по странам поставщиков
SELECT
    country_name,
    SUM(total_revenue) AS country_revenue
FROM bigdata.supplier_sales_mart
GROUP BY country_name
ORDER BY country_revenue DESC;

-- 6.1 Продукты с наивысшим и наименьшим рейтингом
-- Топ-10 по наивысшему рейтингу
SELECT
    product_name,
    avg_rating
FROM bigdata.product_quality_mart
ORDER BY avg_rating DESC
LIMIT 10;

-- Топ-10 по наименьшему рейтингу
SELECT
    product_name,
    avg_rating
FROM bigdata.product_quality_mart
ORDER BY avg_rating ASC
LIMIT 10;

-- 6.2 Корреляция между рейтингом и объемом продаж
SELECT
    corr(avg_rating, total_quantity_sold) AS rating_sales_correlation
FROM bigdata.product_quality_mart;

-- 6.3 Продукты с наибольшим количеством отзывов
SELECT
    product_name,
    total_reviews
FROM bigdata.product_quality_mart
ORDER BY total_reviews DESC
LIMIT 10;