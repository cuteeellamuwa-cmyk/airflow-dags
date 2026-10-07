package com.cute.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.types.RowKind;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.catalog.TableIdentifier;

public class OrdersVersionFilter {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(10000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("orderscdc")
                .setGroupId("ordersicebergstate")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<OrderEvent> events = env
                .fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "orders-version-kafka-source"
                )
                .map(OrdersVersionFilter::parseDebezium)
                .filter(event -> event != null);

        DataStream<OrderEvent> filtered = events
                .keyBy((KeySelector<OrderEvent, Long>) event -> event.id)
                .process(new VersionFilter());

        DataStream<RowData> icebergRows = filtered
                .map(OrdersVersionFilter::toRowData);

        Configuration hadoopConf = new Configuration();

        hadoopConf.set(
                "fs.defaultFS",
                "hdfs://hdfs-namenode:9000"
        );

        hadoopConf.set(
                "hive.metastore.uris",
                "thrift://hive-metastore:9083"
        );

        TableLoader tableLoader =
                TableLoader.fromCatalog(
                        org.apache.iceberg.flink.CatalogLoader.hive(
                                "iceberg_catalog",
                                hadoopConf,
                                java.util.Map.of(
                                        "uri",
                                        "thrift://hive-metastore:9083",
                                        "warehouse",
                                        "hdfs://hdfs-namenode:9000/warehouse/iceberg"
                                )
                        ),
                        TableIdentifier.of(
                                "cute",
                                "ordersiceberg"
                        )
                );

        FlinkSink.forRowData(icebergRows)
                .tableLoader(tableLoader)
                .upsert(true)
                .append();

        env.execute("ordersversionfilter");
    }

    private static OrderEvent parseDebezium(String json) {

        try {
            JsonNode root = MAPPER.readTree(json);

            String op = root.path("op").asText();

            JsonNode row;

            // DELETE 的 after=null，所以读取 before
            // INSERT / UPDATE 读取 after
            if ("d".equals(op)) {
                row = root.get("before");
            } else {
                row = root.get("after");
            }

            if (row == null || row.isNull()) {
                return null;
            }

            OrderEvent event = new OrderEvent();

            event.id = row.path("id").asLong();
            event.orderNo = getText(row, "order_no");
            event.userId = getLong(row, "user_id");
            event.productId = getLong(row, "product_id");

            event.category = getText(row, "category");
            event.region = getText(row, "region");

            event.quantity = getInt(row, "quantity");

            event.unitPrice = getDecimal(row, "unit_price");
            event.totalAmount = getDecimal(row, "total_amount");

            event.orderStatus = getText(row, "order_status");
            event.paymentStatus = getText(row, "payment_status");

            event.eventTime = getDateTime(row, "event_time");
            event.createdAt = getDateTime(row, "created_at");
            event.updatedAt = getDateTime(row, "updated_at");

            event.version = row.path("version").asInt();

            // c = INSERT, u = UPDATE, d = DELETE
            event.op = op;

            return event;

        } catch (Exception e) {
            System.err.println("JSON parse failed: " + json);
            e.printStackTrace();
            return null;
        }
    }

    private static String getText(JsonNode row, String field) {
        JsonNode node = row.get(field);

        if (node == null || node.isNull()) {
            return null;
        }

        return node.asText();
    }

    private static Long getLong(JsonNode row, String field) {
        JsonNode node = row.get(field);

        if (node == null || node.isNull()) {
            return null;
        }

        return node.asLong();
    }

    private static Integer getInt(JsonNode row, String field) {
        JsonNode node = row.get(field);

        if (node == null || node.isNull()) {
            return null;
        }

        return node.asInt();
    }

    private static BigDecimal getDecimal(JsonNode row, String field) {
        JsonNode node = row.get(field);

        if (node == null || node.isNull()) {
            return null;
        }

        return node.decimalValue();
    }

    private static LocalDateTime getDateTime(JsonNode row, String field) {
        JsonNode node = row.get(field);

        if (node == null || node.isNull()) {
            return null;
        }

        return LocalDateTime.parse(
                node.asText().replace(" ", "T")
        );
    }


    private static RowData toRowData(OrderEvent event) {

        GenericRowData row = new GenericRowData(16);

        // Debezium CDC op -> Flink RowKind
        if ("c".equals(event.op)) {
            row.setRowKind(RowKind.INSERT);

        } else if ("u".equals(event.op)) {
            row.setRowKind(RowKind.UPDATE_AFTER);

        } else if ("d".equals(event.op)) {
            row.setRowKind(RowKind.DELETE);

        } else {
            throw new IllegalArgumentException(
                    "Unsupported CDC op: " + event.op
            );
        }

        // 字段顺序必须和 Iceberg 表完全一致
        row.setField(0, event.id);
        row.setField(1, stringData(event.orderNo));
        row.setField(2, event.userId);
        row.setField(3, event.productId);
        row.setField(4, stringData(event.category));
        row.setField(5, stringData(event.region));
        row.setField(6, event.quantity);

        row.setField(
                7,
                event.unitPrice == null
                        ? null
                        : DecimalData.fromBigDecimal(
                                event.unitPrice,
                                18,
                                2
                        )
        );

        row.setField(
                8,
                event.totalAmount == null
                        ? null
                        : DecimalData.fromBigDecimal(
                                event.totalAmount,
                                18,
                                2
                        )
        );

        row.setField(9, stringData(event.orderStatus));
        row.setField(10, stringData(event.paymentStatus));

        row.setField(
                11,
                event.eventTime == null
                        ? null
                        : TimestampData.fromLocalDateTime(event.eventTime)
        );

        row.setField(
                12,
                event.createdAt == null
                        ? null
                        : TimestampData.fromLocalDateTime(event.createdAt)
        );

        row.setField(
                13,
                event.updatedAt == null
                        ? null
                        : TimestampData.fromLocalDateTime(event.updatedAt)
        );

        row.setField(14, event.version);

        row.setField(
                15,
                event.createdAt == null
                        ? null
                        : StringData.fromString(
                                event.createdAt.toLocalDate().toString()
                        )
        );

        return row;
    }

    private static StringData stringData(String value) {

        if (value == null) {
            return null;
        }

        return StringData.fromString(value);
    }


    public static class VersionFilter
            extends KeyedProcessFunction<Long, OrderEvent, OrderEvent> {

        private transient ValueState<Integer> maxVersion;

        @Override
        public void open(OpenContext openContext) throws Exception {

            ValueStateDescriptor<Integer> descriptor =
                    new ValueStateDescriptor<>(
                            "maxVersion",
                            Integer.class
                    );

            maxVersion = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(
                OrderEvent event,
                Context ctx,
                Collector<OrderEvent> out) throws Exception {

            Integer currentMax = maxVersion.value();

            // DELETE 单独处理
            if ("d".equals(event.op)) {

                // 没有历史 State 时，仍允许 DELETE
                if (currentMax == null) {
                    maxVersion.update(event.version);
                    out.collect(event);

                    System.out.println(
                            "ACCEPT delete first"
                                    + " id=" + event.id
                                    + " version=" + event.version
                    );
                    return;
                }

                // DELETE 的 version 不低于当前最大版本，允许删除
                if (event.version >= currentMax) {
                    maxVersion.update(event.version);

                    // 注意：这里不 clear State
                    out.collect(event);

                    System.out.println(
                            "ACCEPT delete"
                                    + " id=" + event.id
                                    + " version=" + event.version
                                    + " maxVersion=" + currentMax
                    );
                } else {
                    System.out.println(
                            "DROP old delete"
                                    + " id=" + event.id
                                    + " deleteVersion=" + event.version
                                    + " maxVersion=" + currentMax
                    );
                }

                return;
            }

            // INSERT / UPDATE
            if (currentMax == null) {
                maxVersion.update(event.version);
                out.collect(event);

                System.out.println(
                        "ACCEPT first event"
                                + " id=" + event.id
                                + " version=" + event.version
                );
                return;
            }

            if (event.version > currentMax) {
                maxVersion.update(event.version);
                out.collect(event);

                System.out.println(
                        "ACCEPT newer version"
                                + " id=" + event.id
                                + " version=" + event.version
                                + " previousMax=" + currentMax
                );

            } else if (event.version == currentMax) {

                System.out.println(
                        "DROP duplicate version"
                                + " id=" + event.id
                                + " version=" + event.version
                );

            } else {

                System.out.println(
                        "DROP old version"
                                + " id=" + event.id
                                + " incomingVersion=" + event.version
                                + " maxVersion=" + currentMax
                );
            }
        }
    }

    public static class OrderEvent {

        public long id;

        public String orderNo;
        public Long userId;
        public Long productId;

        public String category;
        public String region;

        public Integer quantity;

        public BigDecimal unitPrice;
        public BigDecimal totalAmount;

        public String orderStatus;
        public String paymentStatus;

        public LocalDateTime eventTime;
        public LocalDateTime createdAt;
        public LocalDateTime updatedAt;

        public int version;

        // Debezium CDC 操作：
        // c = INSERT
        // u = UPDATE
        // d = DELETE
        public String op;

        public OrderEvent() {
        }

    }
}
