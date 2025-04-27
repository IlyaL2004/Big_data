
CREATE TABLE mock_data_raw (
  id SERIAL PRIMARY KEY,
  customer_first_name VARCHAR,
  customer_last_name VARCHAR,
  customer_age INT,
  customer_email VARCHAR,
  customer_country VARCHAR,
  customer_postal_code VARCHAR,
  customer_pet_type VARCHAR,
  customer_pet_name VARCHAR,
  customer_pet_breed VARCHAR,
  seller_first_name VARCHAR,
  seller_last_name VARCHAR,
  seller_email VARCHAR,
  seller_country VARCHAR,
  seller_postal_code VARCHAR,
  product_name VARCHAR,
  product_category VARCHAR,
  product_price FLOAT,
  product_quantity INT,
  sale_date VARCHAR,
  sale_customer_id INT,
  sale_seller_id INT,
  sale_product_id INT,
  sale_quantity INT,
  sale_total_price FLOAT,
  store_name VARCHAR,
  store_location VARCHAR,
  store_city VARCHAR,
  store_state VARCHAR,
  store_country VARCHAR,
  store_phone VARCHAR,
  store_email VARCHAR,
  pet_category VARCHAR,
  product_weight FLOAT,
  product_color VARCHAR,
  product_size VARCHAR,
  product_brand VARCHAR,
  product_material VARCHAR,
  product_description TEXT,
  product_rating FLOAT,
  product_reviews INT,
  product_release_date VARCHAR,
  product_expiry_date VARCHAR,
  supplier_name VARCHAR,
  supplier_contact VARCHAR,
  supplier_email VARCHAR,
  supplier_phone VARCHAR,
  supplier_address TEXT,
  supplier_city VARCHAR,
  supplier_country VARCHAR
);

CREATE TABLE dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    age INT,
    postal_code VARCHAR(20),
    country_id INT REFERENCES dim_country(country_id)
);

CREATE TABLE dim_pet_type (
    pet_type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_pet_breed (
    pet_breed_id SERIAL PRIMARY KEY,
    breed_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_pet_category (
    pet_category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_pet (
    pet_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES dim_customer(customer_id),
    pet_name VARCHAR(100),
    pet_type_id INT REFERENCES dim_pet_type(pet_type_id),
    pet_breed_id INT REFERENCES dim_pet_breed(pet_breed_id),
    pet_category_id INT REFERENCES dim_pet_category(pet_category_id)
);

CREATE TABLE dim_seller (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    postal_code VARCHAR(20),
    country_id INT REFERENCES dim_country(country_id)
);

CREATE TABLE dim_product_category (
    category_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_brand (
    brand_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_material (
    material_id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE dim_color (
    color_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE dim_size (
    size_id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE dim_product (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    category_id INT REFERENCES dim_product_category(category_id),
    brand_id INT REFERENCES dim_brand(brand_id),
    material_id INT REFERENCES dim_material(material_id),
    color_id INT REFERENCES dim_color(color_id),
    size_id INT REFERENCES dim_size(size_id),
    weight FLOAT,
    description TEXT,
    rating FLOAT,
    reviews INT,
    release_date DATE,
    expiry_date DATE
);

CREATE TABLE dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    contact_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country_id INT REFERENCES dim_country(country_id)
);

CREATE TABLE dim_store (
    store_id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country_id INT REFERENCES dim_country(country_id),
    phone VARCHAR(50),
    email VARCHAR(255)
);

CREATE TABLE dim_date (
    date DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    weekday VARCHAR(20)
);

CREATE TABLE sales_fact (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE REFERENCES dim_date(date),
    customer_id INT REFERENCES dim_customer(customer_id),
    seller_id INT REFERENCES dim_seller(seller_id),
    product_id INT REFERENCES dim_product(product_id),
    store_id INT REFERENCES dim_store(store_id),
    supplier_id INT REFERENCES dim_supplier(supplier_id),
    quantity INT,
    total_price FLOAT
);
