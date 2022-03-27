package com.example;


import java.nio.file.Paths;
import java.util.concurrent.TimeoutException;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

/**
 * Simple local Spark job readin
 */
public class SparkDeltaLakeConsumerJob {

    public static String PROJECT_NAME = "flink-delta-lake-example-project";
    public static String EXAMPLE_TABLE_PATH = "example_table_path";
    static String TABLE_PATH = resolveExampleTableAbsolutePath();

    public static void main(String[] args) throws Exception {
        new SparkDeltaLakeConsumerJob().run();
    }

    public void run() throws TimeoutException, StreamingQueryException {
        SparkSession spark = getLocalSparkSession();

        StreamingQuery streamingQuery = spark
            .readStream()
            .format("delta")
            .load(TABLE_PATH)
            .writeStream()
            .format("console")
            .start();

        streamingQuery.awaitTermination();
    }

    private SparkSession getLocalSparkSession() {
        SparkConf sparkConf = new SparkConf()
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.ui.enabled", "false");

        return SparkSession
            .builder()
            .config(sparkConf)
            .master("local[*]")
            .appName("Spark DeltaLake Consumer Job")
            .getOrCreate();
    }

    static String resolveExampleTableAbsolutePath() {
        String currentPath = Paths.get(".").toAbsolutePath().normalize().toString();
        String rootProjectPath = currentPath.split(PROJECT_NAME)[0] + PROJECT_NAME;
        return rootProjectPath + "/" + EXAMPLE_TABLE_PATH;
    }
}

