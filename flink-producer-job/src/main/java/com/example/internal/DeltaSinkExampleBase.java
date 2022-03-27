package com.example.internal;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import com.example.FlinkDeltaLakeProducerJob;
import io.delta.flink.sink.DeltaSink;
import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;

public abstract class DeltaSinkExampleBase implements DeltaSinkLocalJobRunner {

    public static String PROJECT_NAME = "flink-delta-lake-example-project";
    public static String EXAMPLE_TABLE_PATH = "example_table_path";

    public static String resolveExampleTableAbsolutePath() {
        String currentPath = Paths.get(".").toAbsolutePath().normalize().toString();
        String rootProjectPath = currentPath.split(PROJECT_NAME)[0] + PROJECT_NAME;
        return rootProjectPath + "/" + EXAMPLE_TABLE_PATH;
    }

    public void run(String tablePath) throws IOException {
        System.out.println("Will use table path: " + tablePath);
        File tableDir = new File(tablePath);
        if (FlinkDeltaLakeProducerJob.AUTO_CLEAN_TABLE_PATH && tableDir.list().length > 0) {
            FileUtils.cleanDirectory(tableDir);
        }
        StreamExecutionEnvironment env = getFlinkStreamExecutionEnvironment();
        runFlinkJob(env);
    }

    public abstract DeltaSink<RowData> getDeltaSink();

    private StreamExecutionEnvironment getFlinkStreamExecutionEnvironment() {
        DeltaSink<RowData> deltaSink = getDeltaSink();
        StreamExecutionEnvironment env = getStreamExecutionEnvironment();
        env.addSource(new DeltaSinkExampleSourceFunction()).setParallelism(2).sinkTo(deltaSink).setParallelism(3);
        return env;
    }
}
