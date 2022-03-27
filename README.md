# Apache Flink DeltaLake connector - example project
This is a simple example project showing how to use [offical Flink DeltaLake connector](https://github.com/delta-io/connectors/tree/master/flink-connector) to write data from Apache Flink job to a DeltaLake table.
 

This project consists of two submodules:
- [flink-producer-job](flink-producer-job/): a simple Apache Flink job (with local SourceFunction and embedded local Flink cluster) showing how to produce records using Flink DeltaSink
- [spark-consumer-job](spark-consumer-job/): a one-class Apache Spark Streaming job reading records from a DeltaLake table created by above [flink-producer-job](flink-producer-job/)

## Setting up the test
- you will need a working Maven installation and Java on your local machine in order to run the test jobs,
- make sure that you provided desired versions of the libraries you want to use (eg. Apache Spark, Apache Flink, DeltaLake, Flink Delta Connector) in the [flink producer's pom.xml](flink-producer-job/pom.xml) and [spark consumer's pom.xml](spark-consumer-job/pom.xml) files,
- if any of the required libraries or its versions is missing from the Central Maven repository, then you may install it to your local Maven repository. eg. for the Flink Delta connector (which is not released yet) you can find proper instructions in the project's [README.md](https://github.com/delta-io/connectors/blob/master/flink-connector/README.md).


## Run Flink DeltaSink

Run example in-memory Flink job writing data a Delta table using:
- your local IDE by simply running [FlinkDeltaLakeProducerJob.java](flink-producer-job/src/main/java/com/example/FlinkDeltaLakeProducerJob.java) class that contains `main` method
- local Maven command:
> cd flink-producer-job/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=com.example.FlinkDeltaLakeProducerJob

Above will spawn a local Flink job that will start writing data to the predefined Delta table path of [example_table_path/](example_table_path/). You may then choose to keep the job running or terminate the process  after confirming that a proper ` example_table_path/_delta_log/` directory has been succesfully created (which will mean that some files have been already committed and the test succeeded).

NOTE:
- [example_table_path/](example_table_path/) will not clean itself automatically. If you run the [flink-producer-job](flink-producer-job/) app for the first time then it will create a new table under [example_table_path/](example_table_path/) and each consecutive run of the job will keep appending data to the existing table,
- if you want to automatically clean [example_table_path/](example_table_path/) directory BEFORE each run of the [flink-producer-job](flink-producer-job/) then you can set `FlinkDeltaLakeProducerJob#AUTO_CLEAN_TABLE_PATH` variable to `true` (or you can clean it manually as well)

## Verify by running example Spark Streaming DeltaLake consumer job
After performing above steps you can run prepared [spark-consumer-job](spark-consumer-job/) which is a standalone Spark Streaming job configured to read data from a DeltaLake table existing under [example_table_path/](example_table_path/). If in the previous step you've left your [flink-producer-job](flink-producer-job/) process running then this Spark job will continue to 1 record
in the interval of 800 millis (by default) and output it to the std out. If you've terminated your Flink app then it will simply read all the written records at once and then keep listening onto new ones until you terminate the process

To run example in-memory Spark Streaming job reading data from the existing Delta table:
- use your local IDE by simply running [SparkDeltaLakeConsumerJob.java](spark-consumer-job/src/main/java/com.example/SparkDeltaLakeConsumerJob.java) class that contains `main` method,
- run local Maven command:
> cd spark-consumer-job/
>
> mvn package exec:java -Dexec.cleanupDaemonThreads=false -Dexec.mainClass=com.example.SparkDeltaLakeConsumerJob
