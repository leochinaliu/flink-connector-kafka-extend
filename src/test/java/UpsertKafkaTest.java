import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class UpsertKafkaTest {
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

    public void generateTestData(String topic, String topic2) throws Exception {

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
                        + " (1, 'name 1-2', TIMESTAMP '2020-03-15 13:12:11.123')";
        tEnv.executeSql(initialValues).await();


        KafkaTableTestUtils.createTestTopic(topic2, 1, 1);
        String createTable2 = String.format(
                "CREATE TABLE car (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `car` STRING,\n"
                        + "  `sellTime` TIMESTAMP(3)\n"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'format' = 'json'\n"
                        + ")",
                topic2, kafkaAddress);

        tEnv.executeSql(createTable2);

        String initialValues2 =
                "INSERT INTO car\n"
                        + "VALUES\n"
                        + " (1, 'car 1', TIMESTAMP '2020-03-09 13:12:11.123'),\n"
                        + " (2, 'car 2', TIMESTAMP '2020-03-09 14:12:11.123'),\n"
                        + " (3, 'car 3', TIMESTAMP '2020-03-10 13:12:11.123'),\n"
                        + " (4, 'car 4', TIMESTAMP '2020-03-11 13:12:11.123'),\n"
                        + " (5, 'car 5', TIMESTAMP '2020-03-11 14:12:11.123'),\n"
                        + " (1, 'car 1-2', TIMESTAMP '2020-03-17 13:12:11.123')";
        tEnv.executeSql(initialValues2).await();


    }

    /**
     * 普通的kafka由于没有周期性上涨水印，不能输出数据
     * @throws Exception
     */
    @Test
    public void testNormalJoin() throws Exception {
        joinTable(false);
    }

    /**
     * extend-kafka有周期性上涨水印，可以输出数据
     * @throws Exception
     */
    @Test
    public void testExtendJoin() throws Exception {
        joinTable(true);
    }

    private void joinTable(boolean isExtend) throws Exception {
        String topic = "test-person-birthday2";
        String topic2 = "test-person-car";

        generateTestData(topic, topic2);

        //kafka-extend
        String createTable = String.format(
                "CREATE TABLE upsert_kafka_select (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `birthday` TIMESTAMP(3),\n"
                        + "  PRIMARY KEY (user_id) NOT ENFORCED,"
                        + "  WATERMARK FOR birthday AS birthday  - INTERVAL '10' MINUTE"
                        + ") WITH (\n"
                        + "  'connector' = 'upsert-kafka%s',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = 'test',\n"
                        + "  %s"
                        + "  'key.format' = 'json',\n"
                        + "  'value.format' = 'json'"
                        + ")",
                isExtend ? "-extend" : "", topic, kafkaAddress, isExtend ? "'extend.idleTimeOut' = '60',\n" : "");

        tEnv.executeSql(createTable).await();

        String createTable2 = String.format(
                "CREATE TABLE car_select (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `car` STRING,\n"
                        + "  `sellTime` TIMESTAMP(3),\n"
                        + "  WATERMARK FOR sellTime AS sellTime  - INTERVAL '10' MINUTE"
                        + ") WITH (\n"
                        + "  'connector' = 'kafka%s',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'properties.group.id' = 'test',\n"
                        + "  'scan.startup.mode' = 'earliest-offset',\n"
                        + "  %s"
                        + "  'format' = 'json'"
                        + ")",
                isExtend ? "-extend" : "", topic2, kafkaAddress, isExtend ? "'extend.idleTimeOut' = '60',\n" : "");

        tEnv.executeSql(createTable2).await();


        String query = "SELECT * from car_select inner join upsert_kafka_select FOR SYSTEM_TIME AS OF sellTime AS userTable on car_select.user_id = userTable.user_id";
//        List<Row> rows = KafkaTableTestUtils.collectRows(tEnv.sqlQuery(query), 30);
        tEnv.executeSql(query).print();

        //Assert.assertEquals(rows.size(), 3);  //最后一条数据也能输出，说明watermark已周期性上涨至当前时间


        //kafka
//        String createTable2 = String.format(
//                "CREATE TABLE upsert_kafka_select2 (\n"
//                        + "  `user_id` BIGINT,\n"
//                        + "  `name` STRING,\n"
//                        + "  `birthday` TIMESTAMP(3),\n"
//                        + "  WATERMARK FOR birthday AS birthday  - INTERVAL '10' MINUTE"
//                        + ") WITH (\n"
//                        + "  'connector' = 'kafka',\n"
//                        + "  'topic' = '%s',\n"
//                        + "  'properties.bootstrap.servers' = '%s',\n"
//                        + "  'properties.group.id' = 'test2',\n"
//                        + "  'scan.startup.mode' = 'earliest-offset',\n"
//                        + "  'format' = 'json'"
//                        + ")",
//                topic, kafkaAddress);
//
//        tEnv.executeSql(createTable2).await();
//
//
//        String query2 = "SELECT TUMBLE_START(birthday, INTERVAL '1' DAY) as birthday, " +
//                "  COUNT(1) FROM upsert_kafka_select2 " +
//                "GROUP BY TUMBLE(birthday, INTERVAL '1' DAY)";
//        //TODO 运行停止不了，造成该单元测试不可用
//        List<Row> rows2 = KafkaTableTestUtils.collectRows(tEnv.sqlQuery(query2), 3);
//        Assert.assertEquals(rows2.size(), 2);

        KafkaTableTestUtils.deleteTestTopic(topic);
        KafkaTableTestUtils.deleteTestTopic(topic2);

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


    private static final class TestingSinkFunction implements SinkFunction<RowData> {

        private static final long serialVersionUID = 455430015321124493L;
        private static List<String> rows = new ArrayList<>();

        private final int expectedSize;

        private TestingSinkFunction(int expectedSize) {
            this.expectedSize = expectedSize;
            rows.clear();
        }

        @Override
        public void invoke(RowData value, Context context) {
            rows.add(value.toString());
        }
    }
}
