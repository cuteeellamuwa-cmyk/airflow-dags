-- ============================================================
-- Kafka orderscdc -> Hudi ordershudi
-- ============================================================

CREATE TABLE orderskafkasource (
    id BIGINT NOT NULL,
    order_no STRING NOT NULL,
    user_id BIGINT,
    product_id BIGINT,
    category STRING,
    region STRING,
    quantity INT,
    unit_price DECIMAL(18, 2),
    total_amount DECIMAL(18, 2),
    order_status STRING,
    payment_status STRING,
    event_time TIMESTAMP(3),
    created_at TIMESTAMP(3),
    updated_at TIMESTAMP(3),
    version INT,
    PRIMARY KEY (id) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'orderscdc',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'ordershudisink',
    'value.format' = 'debezium-json',
    'scan.startup.mode' = 'earliest-offset'
);

-- ============================================================
-- Hudi Catalog
-- ============================================================

CREATE CATALOG hoodie_catalog
WITH (
    'type' = 'hudi',
    'mode' = 'hms',
    'hive.conf.dir' = '/opt/bitnami/flink/conf',
    'catalog.path' = 'hdfs://hdfs-namenode:9000/warehouse/hudi'
);

USE CATALOG hoodie_catalog;

USE cute;

INSERT INTO ordershudi
SELECT
    id,
    order_no,
    user_id,
    product_id,
    category,
    region,
    quantity,
    unit_price,
    total_amount,
    order_status,
    payment_status,
    event_time,
    created_at,
    updated_at,
    version,
    CAST(created_at AS DATE) AS event_date
FROM default_catalog.default_database.orderskafkasource;
