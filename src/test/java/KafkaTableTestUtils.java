/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Utils for kafka table tests.
 */
public class KafkaTableTestUtils {

    public static List<Row> collectRows(Table table, int expectedSize) throws Exception {
        final TableResult result = table.execute();
        final List<Row> collectedRows = new ArrayList<>();
        try (CloseableIterator<Row> iterator = result.collect()) {
            while (collectedRows.size() < expectedSize && iterator.hasNext()) {
                collectedRows.add(iterator.next());
            }
        }
        result.getJobClient()
                .ifPresent(
                        jc -> {
                            try {
                                jc.cancel().get(5, TimeUnit.SECONDS);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        return collectedRows;
    }

    public static void createTestTopic(String topic, int numberOfPartitions, int replicationFactor) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaTest.kafkaAddress);  //kafka服务地址

        AdminClient client = KafkaAdminClient.create(props);//创建操作客户端

        try {
            if (client.listTopics().names().get().stream().anyMatch(a -> a.equalsIgnoreCase(topic))) {
                deleteTestTopic(topic);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        //创建名称为test1的topic，有5个分区
        NewTopic topic2 = new NewTopic(topic, numberOfPartitions, (short) replicationFactor);
        client.createTopics(Arrays.asList(topic2));
        client.close();//关闭
    }

    public static void deleteTestTopic(String topic) {
        Properties props = new Properties();
        props.put("bootstrap.servers", KafkaTest.kafkaAddress);  //kafka服务地址
        AdminClient client = KafkaAdminClient.create(props);//创建操作客户端
        client.deleteTopics(Arrays.asList(topic));
        client.close();//关闭
    }
}
