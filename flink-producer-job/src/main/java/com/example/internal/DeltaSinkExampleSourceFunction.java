package com.example.internal;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.ThreadLocalRandom;

import com.example.FlinkDeltaLakeProducerJob;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

/**
 * Internal class providing mock implementation for example stream source.
 * <p>
 * This streaming source will be generating events of type
 * {@link FlinkDeltaLakeProducerJob#ROW_TYPE} with interval of
 * {@link DeltaSinkExampleSourceFunction#NEXT_ROW_INTERVAL_MILLIS} that will be further fed to the
 * Flink job until the parent process is stopped.
 */
public class DeltaSinkExampleSourceFunction extends RichParallelSourceFunction<RowData> {

    static int NEXT_ROW_INTERVAL_MILLIS = 800;

    public static final DataFormatConverters.DataFormatConverter<RowData, Row> CONVERTER =
        DataFormatConverters.getConverterForDataType(
            TypeConversions.fromLogicalToDataType(FlinkDeltaLakeProducerJob.ROW_TYPE)
        );

    private volatile boolean cancelled = false;

    @Override
    public void run(SourceContext<RowData> ctx) throws InterruptedException {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (!cancelled) {

            Integer value = 1;
            RowData row = CONVERTER.toInternal(
                Row.of(
                    value.floatValue(), // float type
                    value, // int type
                    value.toString(), // varchar type
                    value.doubleValue(), // double type
                    false, // boolean type
                    value.byteValue(), // tiny int type
                    value.shortValue(), // small int type
                    value.longValue(), // big int type
                    String.valueOf(value).getBytes(StandardCharsets.UTF_8), // binary type
                    String.valueOf(value).getBytes(StandardCharsets.UTF_8), // varbinary type
                    LocalDateTime.now(ZoneOffset.UTC), // timestamp type
                    Instant.now(), // local zoned timestamp type
                    LocalDate.now(), // date type
                    String.valueOf(value), // char type
                    BigDecimal.valueOf(value), // decimal type
                    new BigDecimal("11.11") // decimal(4,2) type
                )
            );
            ctx.collect(row);
            Thread.sleep(NEXT_ROW_INTERVAL_MILLIS);
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
