-- =========================
-- DIMENSION TABLES
-- =========================

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_sk SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    city VARCHAR(100),
    country VARCHAR(100),

    -- SCD Type 2 columns
    update_ts TIMESTAMP,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_customer_customer_id
ON dim_customer(customer_id);

CREATE INDEX idx_dim_customer_is_current
ON dim_customer(is_current);



CREATE TABLE IF NOT EXISTS dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price NUMERIC(10,2),
    update_ts TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_product_product_id
ON dim_product(product_id);



CREATE TABLE IF NOT EXISTS dim_date (
    date_sk SERIAL PRIMARY KEY,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    day INT
);



-- =========================
-- FACT TABLE
-- =========================

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_sk SERIAL PRIMARY KEY,

    order_id VARCHAR(50) NOT NULL,

    customer_sk INT REFERENCES dim_customer(customer_sk),
    product_sk INT REFERENCES dim_product(product_sk),
    date_sk INT REFERENCES dim_date(date_sk),

    quantity INT,
    total_amount NUMERIC(12,2),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_fact_sales_customer_sk
ON fact_sales(customer_sk);

CREATE INDEX idx_fact_sales_product_sk
ON fact_sales(product_sk);

-- =========================
-- PIPELINE STATE TABLE
-- =========================

CREATE TABLE pipeline_state (
    pipeline_name TEXT PRIMARY KEY,
    last_processed_ts TIMESTAMP
);

INSERT INTO pipeline_state
VALUES ('customer_pipeline', '1900-01-01');

-- =========================
-- Customer Stage TABLE
-- =========================

CREATE TABLE customer_stage (
    customer_id TEXT,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    city TEXT,
    country TEXT,
    update_ts TIMESTAMP
);

-- =========================
-- Index creation
-- =========================

CREATE UNIQUE INDEX unique_current_customer
ON dim_customer(customer_id)
WHERE is_current = true;