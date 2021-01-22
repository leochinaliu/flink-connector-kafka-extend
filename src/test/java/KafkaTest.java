import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.common.protocol.types.Field;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

public class KafkaTest {
    public static final String kafkaAddress = "dev-bigdata01.aiads-host.com:9092,dev-bigdata02.aiads-host.com:9092,dev-bigdata03.aiads-host.com:9092";

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // set default parallelism to 1
        env.getConfig().setAutoWatermarkInterval(60000);
        tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    }

    public void generateTestData(String topic) throws Exception {

        KafkaTableTestUtils.createTestTopic(topic, 1, 1);

        String createTable = String.format(
                "CREATE TABLE upsert_kafka (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `birthday` TIMESTAMP(3),\n"
                        + "  PRIMARY KEY (user_id) NOT ENFORCED"
                        + ") WITH (\n"
                        + "  'connector' = 'upsert-kafka',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'key.format' = 'json',\n"
                        + "  'value.format' = 'json'\n"
                        + ")",
                topic, kafkaAddress);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO upsert_kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-08 14:12:11.123'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-09 13:12:11.123'),\n"
                        + " (4, 'name 4', TIMESTAMP '2020-03-10 13:12:11.123'),\n"
                        + " (5, 'name 5', TIMESTAMP '2020-03-10 14:12:11.123'),\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-10 13:12:11.123')";
        tEnv.executeSql(initialValues).await();

//        String deleteOneValue = "DELETE FROM upsert_kafka WHERE user_id = 5";
//        tEnv.executeSql(deleteOneValue).await();
    }

    @Test
    public void testTUMBLE() throws Exception {
        String topic = "test-person-birthday2";

        generateTestData(topic);

        //kafka-extend
        String createTable = String.format(
                "CREATE TABLE upsert_kafka_select (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `birthday` TIMESTAMP(3),\n"
                        + "  WATERMARK FOR birthday AS birthday  - INTERVAL '10' MINUTE"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka-extend',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = 'test',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'extend.idleTimeOut' = '60',\n"
                        + "  'format' = 'json'"
                        + ")",
                topic, kafkaAddress);

        tEnv.executeSql(createTable).await();


        String query = "SELECT TUMBLE_START(birthday, INTERVAL '1' DAY) as birthday, " +
                "  COUNT(1) FROM upsert_kafka_select " +
                "GROUP BY TUMBLE(birthday, INTERVAL '1' DAY)";
        List<Row> rows = KafkaTableTestUtils.collectRows(tEnv.sqlQuery(query), 3);

        Assert.assertEquals(rows.size(), 3);  //最后一条数据也能输出，说明watermark已周期性上涨至当前时间


        //kafka
        String createTable2 = String.format(
                "CREATE TABLE upsert_kafka_select2 (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `birthday` TIMESTAMP(3),\n"
                        + "  WATERMARK FOR birthday AS birthday  - INTERVAL '10' MINUTE"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = 'test2',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'format' = 'json'"
                        + ")",
                topic, kafkaAddress);

        tEnv.executeSql(createTable2).await();


        String query2 = "SELECT TUMBLE_START(birthday, INTERVAL '1' DAY) as birthday, " +
                "  COUNT(1) FROM upsert_kafka_select2 " +
                "GROUP BY TUMBLE(birthday, INTERVAL '1' DAY)";
        //TODO 运行停止不了，造成该单元测试不可用
        List<Row> rows2 = KafkaTableTestUtils.collectRows(tEnv.sqlQuery(query2), 3);
        Assert.assertEquals(rows2.size(), 2);

        KafkaTableTestUtils.deleteTestTopic(topic);

//        DataStream<RowData> result = tEnv.toAppendStream(tEnv.sqlQuery(query2), RowData.class);
//        TestingSinkFunction sink = new TestingSinkFunction(2);
//        result.addSink(sink).setParallelism(1);
//
//        try {
//            env.execute("Job_2");
//        } catch (Throwable e) {
//            throw e;
//        }
//
//        System.out.println(TestingSinkFunction.rows);

    }

    /**
     * 该测试不能吐出数据，因为不支持计算列
     * @throws Exception
     */
    @Test
    public void testCompareColumn() throws Exception {
        String topic = "test-person-birthday2";

        generateTestData(topic);

        //kafka-extend
        String createTable = String.format(
                "CREATE TABLE upsert_kafka_select (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `birthday` TIMESTAMP(3),\n"
                        + "  `tmpTime` AS TIMESTAMPADD(DAY, -10, LOCALTIMESTAMP),\n"
                        + "  WATERMARK FOR tmpTime AS tmpTime  - INTERVAL '10' MINUTE"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka-extend',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = 'test',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  'extend.idleTimeOut' = '60',\n"
                        + "  'format' = 'json'"
                        + ")",
                topic, kafkaAddress);

        tEnv.executeSql(createTable).await();


        String query = "SELECT TUMBLE_START(tmpTime, INTERVAL '1' DAY) as tmpTime, " +
                "  COUNT(1) FROM upsert_kafka_select " +
                "GROUP BY TUMBLE(tmpTime, INTERVAL '1' DAY)";
        List<Row> rows = KafkaTableTestUtils.collectRows(tEnv.sqlQuery(query), 3);

        KafkaTableTestUtils.deleteTestTopic(topic);

        Assert.assertEquals(rows.size(), 3);
    }


    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(RowData value, SinkFunction.Context context) {
            rows.add(value.toString());
        }
    }
}
