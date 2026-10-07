SET 'pipeline.name' = 'ordersiceberg';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://hive-metastore:9083',
    'warehouse' = 'hdfs://hdfs-namenode:9000/warehouse/iceberg'
);

CREATE TEMPORARY TABLE orderskafkasource (
    id              BIGINT,
    order_no        STRING,
    user_id         BIGINT,
    product_id      BIGINT,
    category        STRING,
    region          STRING,
    quantity        INT,
    unit_price      DECIMAL(18,2),
    total_amount    DECIMAL(18,2),
    order_status    STRING,
    payment_status  STRING,
    event_time      TIMESTAMP(3),
    created_at      TIMESTAMP(3),
    updated_at      TIMESTAMP(3),
    version         INT,
    PRIMARY KEY (id) NOT ENFORCED
)
WITH (
    'connector' = 'kafka',
    'topic' = 'orderscdc',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'ordersicebergsink',
    'scan.startup.mode' = 'earliest-offset',
    'value.format' = 'debezium-json'
);

INSERT INTO iceberg_catalog.cute.ordersiceberg
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
    DATE_FORMAT(created_at, 'yyyy-MM-dd') AS event_date
FROM orderskafkasource;
