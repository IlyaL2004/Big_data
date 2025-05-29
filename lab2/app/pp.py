from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def write_to_clickhouse(df, table):
    df.write \
        .format("jdbc") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("url", "jdbc:clickhouse://clickhouse:8123/bigdata") \
        .option("dbtable", table) \
        .option("user", "admin") \
        .option("password", "admin") \
        .option("createTableOptions", "ENGINE=MergeTree() ORDER BY tuple()") \
        .mode("overwrite") \
        .save()


def main():
    spark = SparkSession.builder \
        .appName("SalesMartETL") \
        .config("spark.jars", "/jars/postgresql-42.6.0.jar,/jars/clickhouse-jdbc-0.4.6-all.jar") \
        .getOrCreate()

    # Чтение данных из PostgreSQL
    jdbc_url = "jdbc:postgresql://postgres_bigdata:5432/bigdata"
    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000"
    }

    # Список таблиц для чтения
    tables = [
        "sales_fact", "dim_product", "dim_product_category",
        "dim_customer", "dim_country", "dim_date",
        "dim_store", "dim_supplier"
    ]

    # Чтение всех таблиц с явным указанием драйвера
    for table in tables:
        df = spark.read.jdbc(url=jdbc_url, table=table, properties=properties)
        df.createOrReplaceTempView(table)

    # 1. Витрина продаж по продуктам
    product_mart = spark.sql("""
    SELECT 
        p.product_id,
        p.name AS product_name,
        pc.name AS category_name,
        SUM(sf.quantity) AS total_quantity_sold,
        SUM(sf.total_price) AS total_revenue,
        ROUND(AVG(p.rating), 2) AS avg_rating,
        SUM(p.reviews) AS total_reviews
    FROM sales_fact sf
    JOIN dim_product p ON sf.product_id = p.product_id
    JOIN dim_product_category pc ON p.category_id = pc.category_id
    GROUP BY p.product_id, p.name, pc.name
    """)
    write_to_clickhouse(product_mart, "product_sales_mart")
    print("Successfully wrote to product_sales_mart")

    # 2. Витрина продаж по клиентам
    customer_mart = spark.sql("""
    SELECT 
        c.customer_id,
        c.first_name,
        c.last_name,
        co.country_name,
        SUM(sf.total_price) AS total_purchase,
        ROUND(AVG(sf.total_price), 2) AS average_check
    FROM sales_fact sf
    JOIN dim_customer c ON sf.customer_id = c.customer_id
    JOIN dim_country co ON c.country_id = co.country_id
    GROUP BY c.customer_id, c.first_name, c.last_name, co.country_name
    ORDER BY total_purchase DESC
    LIMIT 10
    """)
    write_to_clickhouse(customer_mart, "customer_sales_mart")
    print("Successfully wrote to customer_sales_mart")

    # 3. Витрина продаж по времени
    time_mart = spark.sql("""
    SELECT 
        d.year,
        d.month,
        COUNT(*) AS total_sales,
        SUM(sf.total_price) AS total_revenue,
        ROUND(AVG(sf.total_price), 2) AS avg_order_value
    FROM sales_fact sf
    JOIN dim_date d ON sf.sale_date = d.date
    GROUP BY d.year, d.month
    ORDER BY d.year, d.month
    """)
    write_to_clickhouse(time_mart, "time_sales_mart")
    print("Successfully wrote to time_sales_mart")

    # 4. Витрина продаж по магазинам
    store_mart = spark.sql("""
    SELECT 
        s.store_id,
        s.name AS store_name,
        s.city,
        co.country_name,
        SUM(sf.total_price) AS total_revenue,
        ROUND(AVG(sf.total_price), 2) AS average_check
    FROM sales_fact sf
    JOIN dim_store s ON sf.store_id = s.store_id
    JOIN dim_country co ON s.country_id = co.country_id
    GROUP BY s.store_id, s.name, s.city, co.country_name
    ORDER BY total_revenue DESC
    LIMIT 5
    """)
    write_to_clickhouse(store_mart, "store_sales_mart")
    print("Successfully wrote to store_sales_mart")

    # 5. Витрина продаж по поставщикам
    supplier_mart = spark.sql("""
    SELECT 
        sp.supplier_id,
        sp.name AS supplier_name,
        co.country_name,
        SUM(sf.total_price) AS total_revenue,
        ROUND(SUM(sf.total_price) / SUM(sf.quantity), 2) AS avg_product_price
    FROM sales_fact sf
    JOIN dim_supplier sp ON sf.supplier_id = sp.supplier_id
    JOIN dim_country co ON sp.country_id = co.country_id
    GROUP BY sp.supplier_id, sp.name, co.country_name
    ORDER BY total_revenue DESC
    LIMIT 5
    """)
    write_to_clickhouse(supplier_mart, "supplier_sales_mart")
    print("Successfully wrote to supplier_sales_mart")

    # 6. Витрина качества продукции
    quality_mart = spark.sql("""
    SELECT 
        p.product_id,
        p.name AS product_name,
        ROUND(AVG(p.rating), 2) AS avg_rating,
        SUM(p.reviews) AS total_reviews,
        SUM(sf.quantity) AS total_quantity_sold
    FROM sales_fact sf
    JOIN dim_product p ON sf.product_id = p.product_id
    GROUP BY p.product_id, p.name
    """)
    write_to_clickhouse(quality_mart, "product_quality_mart")
    print("Successfully wrote to product_quality_mart")

    spark.stop()


if __name__ == "__main__":
    main()