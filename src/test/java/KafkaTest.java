import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

//@RunWith(Parameterized.class)
public class KafkaTest  {
    private static final String kafkaAddress = "dev-bigdata01.aiads-host.com:9092,dev-bigdata02.aiads-host.com:9092,dev-bigdata03.aiads-host.com:9092";

    protected StreamExecutionEnvironment env;
    protected StreamTableEnvironment tEnv;

    private static final String JSON_FORMAT = "json";
    private static final String CSV_FORMAT = "csv";
    private static final String AVRO_FORMAT = "avro";


//    @Parameterized.Parameter
//    public String format;
//
//    @Parameterized.Parameters(name = "format = {0}")
//    public static Object[] parameters() {
//        return new Object[]{JSON_FORMAT, CSV_FORMAT, AVRO_FORMAT};
//    }

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // set default parallelism to 4
        tEnv = StreamTableEnvironment.create(
                env,
                EnvironmentSettings.newInstance()
                        .useBlinkPlanner()
                        .inStreamingMode()
                        .build());
        env.getConfig().setRestartStrategy(RestartStrategies.noRestart());
    }

    @Test
    public void testSource() throws Exception {
        String createTable = String.format(
                "CREATE TABLE upsert_kafka (\n"
                        + "  `user_id` BIGINT,\n"
                        + "  `name` STRING,\n"
                        + "  `birthday` TIMESTAMP(3),\n"
                        + "  `timestamp` TIMESTAMP(3) METADATA,\n"
                        + "  PRIMARY KEY (user_id) NOT ENFORCED"
                        + ") WITH (\n"
                        + "  'connector' = 'upsert-kafka',\n"
                        + "  'topic' = '%s',\n"
                        + "  'properties.bootstrap.servers' = '%s',\n"
                        + "  'key.format' = 'json',\n"
                        + "  'value.format' = 'json',\n"
                        + "  'sink.parallelism' = '1'"
                        + ")",
                "test-person-birthday", kafkaAddress);

        tEnv.executeSql(createTable);

        String initialValues =
                "INSERT INTO upsert_kafka\n"
                        + "VALUES\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-08 13:12:11.123', TIMESTAMP '2020-03-08 13:12:11.123'),\n"
                        + " (2, 'name 2', TIMESTAMP '2020-03-08 14:12:11.123', TIMESTAMP '2020-03-09 13:12:11.123'),\n"
                        + " (3, 'name 3', TIMESTAMP '2020-03-09 13:12:11.123', TIMESTAMP '2020-03-10 13:12:11.123'),\n"
                        + " (1, 'name 1', TIMESTAMP '2020-03-09 13:12:11.123', TIMESTAMP '2020-03-11 13:12:11.123')";
        tEnv.executeSql(initialValues).await();
    }
}
