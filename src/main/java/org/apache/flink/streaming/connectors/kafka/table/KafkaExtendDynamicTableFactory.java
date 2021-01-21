package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

/**
 * 由于官方的kafka没有提供周期性上涨水印功能，这里提供一个能周期性上涨水印的kafka connector
 */
public class KafkaExtendDynamicTableFactory extends KafkaDynamicTableFactory {
    public static final String IDENTIFIER = "kafka-extend";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        //调用基类的createDynamicTableSource以完成基本的校验等功能
        KafkaDynamicSource source = (KafkaDynamicSource) super.createDynamicTableSource(context);

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

        //替换为新的KafkaDynamicSource，新的KafkaDynamicSource里applyWatermark是个空方法，防止flink planner赋值watermark
        //并增加新逻辑的watermark
        return new DynamicTableTools().extendDynamicTableSource(source, context, helper);
    }
}
