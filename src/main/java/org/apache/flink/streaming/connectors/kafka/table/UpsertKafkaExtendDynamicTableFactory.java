package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Set;

/**
 * 由于官方的upsert-kafka没有提供周期性上涨水印功能，这里提供一个能周期性上涨水印的kafka connector
 */
public class UpsertKafkaExtendDynamicTableFactory extends UpsertKafkaDynamicTableFactory {
    public static final String IDENTIFIER = "upsert-kafka-extend";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionSet = super.optionalOptions();
        optionSet.add(DynamicTableTools.EXTEND_IDLETIMEOUT);
        optionSet.add(DynamicTableTools.EXTEND_WATERMARKINTERVAL);
        return optionSet;
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
