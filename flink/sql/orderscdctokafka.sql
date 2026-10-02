CREATE TABLE ordersource (
    id BIGINT NOT NULL,
    order_no STRING,
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
    'connector' = 'mysql-cdc',
    'hostname' = 'mysql',
    'port' = '3306',
    'username' = '${CDC_USERNAME}',
    'password' = '${CDC_PASSWORD}',
    'database-name' = 'cute',
    'table-name' = 'orders',
    'scan.startup.mode' = 'initial'
);

CREATE TABLE orderscdcsink (
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
    version INT
) WITH (
    'connector' = 'kafka',
    'topic' = 'orderscdc',
    'properties.bootstrap.servers' = 'kafka:9092',

    'key.format' = 'json',
    'key.fields' = 'order_no',

    'value.format' = 'debezium-json',
    'value.fields-include' = 'ALL',

    'properties.acks' = 'all',
    'properties.enable.idempotence' = 'true',

    'properties.linger.ms' = '1000',
    'properties.batch.size' = '32768',
    'properties.compression.type' = 'lz4'
);

INSERT INTO orderscdcsink
SELECT * FROM ordersource;
