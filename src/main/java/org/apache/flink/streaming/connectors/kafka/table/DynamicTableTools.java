package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class DynamicTableTools {
    public static final ConfigOption<Integer> EXTEND_IDLETIMEOUT = ConfigOptions
            .key("extend.idleTimeOut")
            .intType()
            .noDefaultValue()
            .withDescription("空闲超时时间，单位秒，超过此时间水印将自动上涨，默认5分钟");

    public static final ConfigOption<Integer> EXTEND_WATERMARKINTERVAL = ConfigOptions
            .key("extend.watermarkInterval")
            .intType()
            .noDefaultValue()
            .withDescription("水印上涨到离当前时间的间隔，单位秒，默认10分钟");

    /**
     * 替换为新的KafkaDynamicSource，新的KafkaDynamicSource里applyWatermark是个空方法，防止flink planner赋值watermark，并增加新逻辑的watermark
     * @param source
     * @param context
     * @param helper
     * @return
     */
    public DynamicTableSource extendDynamicTableSource(KafkaDynamicSource source, DynamicTableFactory.Context context, FactoryUtil.TableFactoryHelper helper) {
        KafkaDynamicSourceExtend sourceExtend = KafkaDynamicSourceExtend.copyFromBase(source);

        //获取配置
        ReadableConfig tableOptions = helper.getOptions();
        int idletimeout = tableOptions.getOptional(EXTEND_IDLETIMEOUT).orElse(300);
        int watermarkinterval = tableOptions.getOptional(EXTEND_WATERMARKINTERVAL).orElse(600);


        //从tableSchema反推出rowtime字段是第几个字段
        TableSchema tableSchema = context.getCatalogTable().getSchema();
        int rowTimeFieldPosition = -1;
        if (!tableSchema.getWatermarkSpecs().isEmpty()) {
            //目前只支持定义一个watermark
            String rowTimeAttr = tableSchema.getWatermarkSpecs().get(0).getRowtimeAttribute();

            for (int i = 0; i < tableSchema.getTableColumns().size(); i++) {
                if (tableSchema.getTableColumns().get(i).getName().equalsIgnoreCase(rowTimeAttr)) {
                    rowTimeFieldPosition = i;
                    break;
                }
            }
        }else {
            throw new ValidationException("upsert-kafka-extend只支持有设置水印的table");
        }


        sourceExtend.applyWatermark2(createWatermarkStrategy(rowTimeFieldPosition, idletimeout, watermarkinterval));
        return sourceExtend;
    }

    /**
     * 周期性上涨的水印
     * 在配置的空闲时间idletimeout到后，会将水印上涨至当前时间-watermarkinterval。如果比实际事件水印更低则使用事件水印
     * @param rowTimeFieldPosition 发出事件时间的字段的索引
     * @param idletimeout 空闲时间
     * @param watermarkinterval 水印上涨到离当前时间的间隔，单位秒
     * @return
     */
    private WatermarkStrategy<RowData> createWatermarkStrategy(int rowTimeFieldPosition, int idletimeout, int watermarkinterval) {
        if (rowTimeFieldPosition < 0) {
            throw new ValidationException("水印字段的索引不能小于0，这或许是个bug，请联系开发者");
        }

        return new WatermarkStrategy<RowData>() {
            @Override
            public WatermarkGenerator<RowData> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new WatermarkGenerator<RowData>() {
                    private long currentTimestamp = Long.MIN_VALUE;
                    private LocalDateTime lastProcessDataTime = LocalDateTime.now();

                    @Override
                    public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
                        currentTimestamp = Math.max(currentTimestamp, eventTimestamp);
                        lastProcessDataTime = LocalDateTime.now();
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        //检测空闲时间
                        if (lastProcessDataTime.isBefore(LocalDateTime.now().minusSeconds(idletimeout))) {
                            //比较和实际事件时间的大小
                            long ts = LocalDateTime.now().minusSeconds(watermarkinterval).toInstant(ZoneOffset.UTC).toEpochMilli();
                            currentTimestamp = Math.max(currentTimestamp, ts);
                        }

                        output.emitWatermark(new Watermark(currentTimestamp));
                    }
                };
            }
        }.withTimestampAssigner((event, timestamp) -> {
            if (event.getArity() > rowTimeFieldPosition)
                return event.getTimestamp(rowTimeFieldPosition, 3).getMillisecond();
            else
                return Long.MIN_VALUE;
        });
    }
}
