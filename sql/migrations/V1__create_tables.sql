-- V1: Initial Schema Creation

CREATE TABLE dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(150),
    city VARCHAR(100),
    country VARCHAR(100),
    update_ts TIMESTAMP,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN
);

CREATE TABLE dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(200),
    category VARCHAR(100),
    price NUMERIC(12,2)
);

CREATE TABLE dim_date (
    date_sk SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INT,
    quarter INT,
    month INT,
    day INT
);

CREATE TABLE fact_sales (
    sales_sk SERIAL PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    customer_sk INT REFERENCES dim_customer(customer_sk),
    product_sk INT REFERENCES dim_product(product_sk),
    date_sk INT REFERENCES dim_date(date_sk),
    quantity INT,
    total_amount NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_sales_customer_sk ON fact_sales(customer_sk);
CREATE INDEX idx_fact_sales_product_sk ON fact_sales(product_sk);