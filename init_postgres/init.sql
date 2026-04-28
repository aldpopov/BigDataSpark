CREATE SCHEMA IF NOT EXISTS star_schema;

CREATE TABLE star_schema.dim_customer (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    age INTEGER,
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE star_schema.dim_product (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(100),
    price DECIMAL(10,2),
    brand VARCHAR(100),
    rating DECIMAL(3,2),
    reviews INTEGER,
    weight DECIMAL(10,2),
    color VARCHAR(50),
    size VARCHAR(50),
    material VARCHAR(100)
);

CREATE TABLE star_schema.dim_seller (
    seller_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(200),
    country VARCHAR(100),
    postal_code VARCHAR(20)
);

CREATE TABLE star_schema.dim_store (
    store_id SERIAL PRIMARY KEY,
    store_name VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    phone VARCHAR(50),
    email VARCHAR(200)
);

CREATE TABLE star_schema.dim_supplier (
    supplier_id SERIAL PRIMARY KEY,
    supplier_name VARCHAR(200),
    contact VARCHAR(200),
    email VARCHAR(200),
    phone VARCHAR(50),
    address TEXT,
    city VARCHAR(100),
    country VARCHAR(100)
);

CREATE TABLE star_schema.dim_time (
    time_id SERIAL PRIMARY KEY,
    date DATE,
    year INTEGER,
    month INTEGER,
    day INTEGER,
    quarter INTEGER,
    week_day INTEGER
);

CREATE TABLE star_schema.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    customer_id INTEGER REFERENCES star_schema.dim_customer(customer_id),
    product_id INTEGER REFERENCES star_schema.dim_product(product_id),
    seller_id INTEGER REFERENCES star_schema.dim_seller(seller_id),
    store_id INTEGER REFERENCES star_schema.dim_store(store_id),
    supplier_id INTEGER REFERENCES star_schema.dim_supplier(supplier_id),
    time_id INTEGER REFERENCES star_schema.dim_time(time_id),
    quantity INTEGER,
    total_price DECIMAL(10,2),
    sale_date DATE
);