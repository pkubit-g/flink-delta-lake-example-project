package com.example;

import java.util.Arrays;

import com.example.internal.DeltaSinkExampleBase;
import io.delta.flink.sink.DeltaSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.*;
import org.apache.hadoop.conf.Configuration;

/**
 * Demonstrates how the Flink Delta Sink can be used to write data to Delta table.
 * <p>
 * If you run this example then application will spawn example local Flink job generating data to
 * the underlying Delta table under directory of "example_table_path/" in the repository's root
 * path.
 */
public class FlinkDeltaLakeProducerJob extends DeltaSinkExampleBase {

    public static boolean AUTO_CLEAN_TABLE_PATH = false;
    static String TABLE_PATH = resolveExampleTableAbsolutePath();

    public static final RowType ROW_TYPE = new RowType(
        Arrays.asList(
            new RowType.RowField("f1", new FloatType()),
            new RowType.RowField("f2", new IntType()),
            new RowType.RowField("f3", new VarCharType()),
            new RowType.RowField("f4", new DoubleType()),
            new RowType.RowField("f5", new BooleanType()),
            new RowType.RowField("f6", new TinyIntType()),
            new RowType.RowField("f7", new SmallIntType()),
            new RowType.RowField("f8", new BigIntType()),
            new RowType.RowField("f9", new BinaryType()),
            new RowType.RowField("f10", new VarBinaryType()),
            new RowType.RowField("f11", new TimestampType()),
            new RowType.RowField("f12", new LocalZonedTimestampType()),
            new RowType.RowField("f13", new DateType()),
            new RowType.RowField("f14", new CharType()),
            new RowType.RowField("f15", new DecimalType()),
            new RowType.RowField("f16", new DecimalType(4, 2))
        ));

    public static void main(String[] args) throws Exception {
        new FlinkDeltaLakeProducerJob().run(TABLE_PATH);
    }

    @Override
    public DeltaSink<RowData> getDeltaSink() {
        return DeltaSink
            .forRowData(
                new Path(TABLE_PATH),
                new Configuration(),
                ROW_TYPE)
            .build();
    }
}
