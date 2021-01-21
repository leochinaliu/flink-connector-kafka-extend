package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * 重写KafkaDynamicSource，将applyWatermark置为空方法防止Planner设置watermarkStrategy
 * 注意：若新增字段需要在copy方法中增加支持
 */
public class KafkaDynamicSourceExtend extends KafkaDynamicSource {
    public KafkaDynamicSourceExtend(DataType physicalDataType,
                                    @Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
                                    DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
                                    int[] keyProjection,
                                    int[] valueProjection,
                                    @Nullable String keyPrefix,
                                    @Nullable List<String> topics,
                                    @Nullable Pattern topicPattern,
                                    Properties properties,
                                    StartupMode startupMode,
                                    Map<KafkaTopicPartition, Long> specificStartupOffsets,
                                    long startupTimestampMillis, boolean upsertMode) {
        super(physicalDataType, keyDecodingFormat, valueDecodingFormat, keyProjection, valueProjection, keyPrefix, topics,
                topicPattern, properties, startupMode, specificStartupOffsets, startupTimestampMillis, upsertMode);
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        //do nothing
//        this.watermarkStrategy = watermarkStrategy;
    }

    public void applyWatermark2(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public DynamicTableSource copy() {
        final KafkaDynamicSourceExtend copy = new KafkaDynamicSourceExtend(
                physicalDataType,
                keyDecodingFormat,
                valueDecodingFormat,
                keyProjection,
                valueProjection,
                keyPrefix,
                topics,
                topicPattern,
                properties,
                startupMode,
                specificStartupOffsets,
                startupTimestampMillis,
                upsertMode);
        copy.producedDataType = producedDataType;
        copy.metadataKeys = metadataKeys;
        copy.watermarkStrategy = watermarkStrategy;
        return copy;
    }

    public static KafkaDynamicSourceExtend copyFromBase(KafkaDynamicSource source) {
        final KafkaDynamicSourceExtend sourceExtend = new KafkaDynamicSourceExtend(source.physicalDataType, source.keyDecodingFormat, source.valueDecodingFormat,
                source.keyProjection, source.valueProjection, source.keyPrefix, source.topics,
                source.topicPattern, source.properties, source.startupMode, source.specificStartupOffsets,
                source.startupTimestampMillis, source.upsertMode);

        sourceExtend.producedDataType = source.producedDataType;
        sourceExtend.metadataKeys = source.metadataKeys;
        sourceExtend.watermarkStrategy = source.watermarkStrategy;

        return sourceExtend;
    }
}
