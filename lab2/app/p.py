from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, dayofmonth, date_format, explode, array
from pyspark.sql.types import IntegerType


def main():
    spark = SparkSession.builder \
        .appName("Postgres to Snowflake ETL") \
        .config("spark.jars", "/jars/postgresql-42.6.0.jar") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()

    # Конфигурация PostgreSQL
    pg_url = "jdbc:postgresql://postgres_bigdata:5432/bigdata"
    properties = {
        "user": "admin",
        "password": "admin",
        "driver": "org.postgresql.Driver"
    }

    print("Reading raw data from PostgreSQL...")
    raw_df = spark.read.jdbc(url=pg_url, table="mock_data_raw", properties=properties)
    print(f"Raw data count: {raw_df.count()}")

    # ======================== Таблицы измерений ========================
    # dim_country
    print("Processing dim_country...")
    country_cols = ["customer_country", "seller_country", "store_country", "supplier_country"]
    country_df = raw_df.select(
        explode(array(*[col(c).alias("country_name") for c in country_cols])).alias("country_name")
    ).distinct().filter(col("country_name").isNotNull())
    country_df.write.jdbc(url=pg_url, table="dim_country", mode="append", properties=properties)
    print(f"Inserted {country_df.count()} countries")

    # dim_pet_type
    print("Processing dim_pet_type...")
    pet_type_df = raw_df.select(col("customer_pet_type").alias("type_name")) \
        .distinct().filter(col("type_name").isNotNull())
    pet_type_df.write.jdbc(url=pg_url, table="dim_pet_type", mode="append", properties=properties)
    print(f"Inserted {pet_type_df.count()} pet types")

    # dim_pet_breed
    print("Processing dim_pet_breed...")
    pet_breed_df = raw_df.select(col("customer_pet_breed").alias("breed_name")) \
        .distinct().filter(col("breed_name").isNotNull())
    pet_breed_df.write.jdbc(url=pg_url, table="dim_pet_breed", mode="append", properties=properties)
    print(f"Inserted {pet_breed_df.count()} pet breeds")

    # dim_pet_category
    print("Processing dim_pet_category...")
    pet_category_df = raw_df.select(col("pet_category").alias("category_name")) \
        .distinct().filter(col("category_name").isNotNull())
    pet_category_df.write.jdbc(url=pg_url, table="dim_pet_category", mode="append", properties=properties)
    print(f"Inserted {pet_category_df.count()} pet categories")

    # dim_product_category
    print("Processing dim_product_category...")
    product_category_df = raw_df.select(col("product_category").alias("name")) \
        .distinct().filter(col("name").isNotNull())
    product_category_df.write.jdbc(url=pg_url, table="dim_product_category", mode="append", properties=properties)
    print(f"Inserted {product_category_df.count()} product categories")

    # dim_brand
    print("Processing dim_brand...")
    brand_df = raw_df.select(col("product_brand").alias("name")) \
        .distinct().filter(col("name").isNotNull())
    brand_df.write.jdbc(url=pg_url, table="dim_brand", mode="append", properties=properties)
    print(f"Inserted {brand_df.count()} brands")

    # dim_material
    print("Processing dim_material...")
    material_df = raw_df.select(col("product_material").alias("name")) \
        .distinct().filter(col("name").isNotNull())
    material_df.write.jdbc(url=pg_url, table="dim_material", mode="append", properties=properties)
    print(f"Inserted {material_df.count()} materials")

    # dim_color
    print("Processing dim_color...")
    color_df = raw_df.select(col("product_color").alias("name")) \
        .distinct().filter(col("name").isNotNull())
    color_df.write.jdbc(url=pg_url, table="dim_color", mode="append", properties=properties)
    print(f"Inserted {color_df.count()} colors")

    # dim_size
    print("Processing dim_size...")
    size_df = raw_df.select(col("product_size").alias("name")) \
        .distinct().filter(col("name").isNotNull())
    size_df.write.jdbc(url=pg_url, table="dim_size", mode="append", properties=properties)
    print(f"Inserted {size_df.count()} sizes")

    # Загрузка таблиц измерений для связей
    dim_country = spark.read.jdbc(url=pg_url, table="dim_country", properties=properties)
    dim_product_category = spark.read.jdbc(url=pg_url, table="dim_product_category", properties=properties)
    dim_brand = spark.read.jdbc(url=pg_url, table="dim_brand", properties=properties)
    dim_material = spark.read.jdbc(url=pg_url, table="dim_material", properties=properties)
    dim_color = spark.read.jdbc(url=pg_url, table="dim_color", properties=properties)
    dim_size = spark.read.jdbc(url=pg_url, table="dim_size", properties=properties)
    dim_pet_type = spark.read.jdbc(url=pg_url, table="dim_pet_type", properties=properties)
    dim_pet_breed = spark.read.jdbc(url=pg_url, table="dim_pet_breed", properties=properties)
    dim_pet_category_table = spark.read.jdbc(url=pg_url, table="dim_pet_category", properties=properties)

    # dim_customer
    print("Processing dim_customer...")
    customer_df = raw_df \
        .join(dim_country, raw_df.customer_country == dim_country.country_name, "inner") \
        .select(
        col("customer_first_name").alias("first_name"),
        col("customer_last_name").alias("last_name"),
        col("customer_email").alias("email"),
        col("customer_age").alias("age"),
        col("customer_postal_code").alias("postal_code"),
        col("country_id")
    ).distinct().filter(col("email").isNotNull())
    customer_df.write.jdbc(url=pg_url, table="dim_customer", mode="append", properties=properties)
    print(f"Inserted {customer_df.count()} customers")

    # dim_seller
    print("Processing dim_seller...")
    seller_df = raw_df \
        .join(dim_country, raw_df.seller_country == dim_country.country_name, "inner") \
        .select(
        col("seller_first_name").alias("first_name"),
        col("seller_last_name").alias("last_name"),
        col("seller_email").alias("email"),
        col("seller_postal_code").alias("postal_code"),
        col("country_id")
    ).distinct().filter(col("email").isNotNull())
    seller_df.write.jdbc(url=pg_url, table="dim_seller", mode="append", properties=properties)
    print(f"Inserted {seller_df.count()} sellers")

    # dim_store
    print("Processing dim_store...")
    store_df = raw_df \
        .join(dim_country, raw_df.store_country == dim_country.country_name, "inner") \
        .select(
        col("store_name").alias("name"),
        col("store_location").alias("location"),
        col("store_city").alias("city"),
        col("store_state").alias("state"),
        col("country_id"),
        col("store_phone").alias("phone"),
        col("store_email").alias("email")
    ).distinct().filter(col("name").isNotNull())
    store_df.write.jdbc(url=pg_url, table="dim_store", mode="append", properties=properties)
    print(f"Inserted {store_df.count()} stores")

    # dim_supplier
    print("Processing dim_supplier...")
    supplier_df = raw_df \
        .join(dim_country, raw_df.supplier_country == dim_country.country_name, "inner") \
        .select(
        col("supplier_name").alias("name"),
        col("supplier_contact").alias("contact_name"),
        col("supplier_email").alias("email"),
        col("supplier_phone").alias("phone"),
        col("supplier_address").alias("address"),
        col("supplier_city").alias("city"),
        col("country_id")
    ).distinct().filter(col("name").isNotNull())
    supplier_df.write.jdbc(url=pg_url, table="dim_supplier", mode="append", properties=properties)
    print(f"Inserted {supplier_df.count()} suppliers")

    # dim_product
    print("Processing dim_product...")
    product_df = raw_df \
        .join(dim_product_category, raw_df.product_category == dim_product_category.name, "left") \
        .join(dim_brand, raw_df.product_brand == dim_brand.name, "left") \
        .join(dim_material, raw_df.product_material == dim_material.name, "left") \
        .join(dim_color, raw_df.product_color == dim_color.name, "left") \
        .join(dim_size, raw_df.product_size == dim_size.name, "left") \
        .select(
        col("product_name").alias("name"),
        col("category_id"),
        col("brand_id"),
        col("material_id"),
        col("color_id"),
        col("size_id"),
        col("product_weight").alias("weight"),
        col("product_description").alias("description"),
        col("product_rating").alias("rating"),
        col("product_reviews").cast(IntegerType()).alias("reviews"),
        to_date(col("product_release_date"), "M/d/yyyy").alias("release_date"),
        to_date(col("product_expiry_date"), "M/d/yyyy").alias("expiry_date")
    ).distinct().filter(col("name").isNotNull())
    product_df.write.jdbc(url=pg_url, table="dim_product", mode="append", properties=properties)
    print(f"Inserted {product_df.count()} products")

    # Загрузка дополнительных таблиц для dim_pet
    dim_customer = spark.read.jdbc(url=pg_url, table="dim_customer", properties=properties)

    # dim_pet
    print("Processing dim_pet...")
    pet_df = raw_df \
        .join(dim_customer, raw_df.customer_email == dim_customer.email, "inner") \
        .join(dim_pet_type, raw_df.customer_pet_type == dim_pet_type.type_name, "inner") \
        .join(dim_pet_breed, raw_df.customer_pet_breed == dim_pet_breed.breed_name, "inner") \
        .join(dim_pet_category_table, raw_df.pet_category == dim_pet_category_table.category_name, "inner") \
        .select(
        col("customer_id"),
        col("customer_pet_name").alias("pet_name"),
        col("pet_type_id"),
        col("pet_breed_id"),
        col("pet_category_id")
    ).distinct().filter(col("pet_name").isNotNull())

    pet_df.write.jdbc(url=pg_url, table="dim_pet", mode="append", properties=properties)
    print(f"Inserted {pet_df.count()} pets")

    # dim_date
    print("Processing dim_date...")
    date_df = raw_df.select(to_date(col("sale_date"), "M/d/yyyy").alias("date")) \
        .distinct().filter(col("date").isNotNull()) \
        .select(
        col("date"),
        year("date").alias("year"),
        month("date").alias("month"),
        dayofmonth("date").alias("day"),
        date_format("date", "EEEE").alias("weekday")
    )
    date_df.write.jdbc(url=pg_url, table="dim_date", mode="append", properties=properties)
    print(f"Inserted {date_df.count()} dates")

    # Загрузка таблиц для фактов
    dim_customer = spark.read.jdbc(url=pg_url, table="dim_customer", properties=properties)
    dim_seller = spark.read.jdbc(url=pg_url, table="dim_seller", properties=properties)
    dim_store = spark.read.jdbc(url=pg_url, table="dim_store", properties=properties)
    dim_supplier = spark.read.jdbc(url=pg_url, table="dim_supplier", properties=properties)
    dim_product = spark.read.jdbc(url=pg_url, table="dim_product", properties=properties)

    # sales_fact
    print("Processing sales_fact...")
    fact_df = raw_df \
        .join(dim_customer, (raw_df.customer_email == dim_customer.email), "left") \
        .join(dim_seller, (raw_df.seller_email == dim_seller.email), "left") \
        .join(dim_store, (raw_df.store_email == dim_store.email), "left") \
        .join(dim_supplier, (raw_df.supplier_email == dim_supplier.email), "left") \
        .join(
        dim_product,
        (raw_df.product_name == dim_product.name) &
        (raw_df.product_description == dim_product.description) &
        (to_date(raw_df.product_release_date, "M/d/yyyy") == dim_product.release_date) &
        (raw_df.product_rating == dim_product.rating) &
        (raw_df.product_reviews.cast(IntegerType()) == dim_product.reviews),
        "left"
    ) \
        .select(
        to_date(col("sale_date"), "M/d/yyyy").alias("sale_date"),
        col("customer_id"),
        col("seller_id"),
        col("product_id"),
        col("store_id"),
        col("supplier_id"),
        col("sale_quantity").alias("quantity"),
        col("sale_total_price").alias("total_price")
    ).filter(col("sale_date").isNotNull())
    fact_df.write.jdbc(url=pg_url, table="sales_fact", mode="append", properties=properties)
    print(f"Inserted {fact_df.count()} sales facts")

    print("ETL process completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()