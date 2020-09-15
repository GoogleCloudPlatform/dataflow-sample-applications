# Quickstart

In this section we will walk through downloading the sample and running through
a few examples using gradle.

## Setup
* Install
    * You will need to have gradle already installed.
    * Download the repository, git clone https://github.com/GoogleCloudPlatform/dataflow-sample-applications.git
* Build
    * cd dataflow-sample-applications/timeseries-streaming/timeseries-java-applications/
    * ./gradlew build
## Creating metrics from a stream
The following commands will run an example pipeline which uses a generated stream of data. While the pipeline is 
running 1 tick happens every 500 ms, the function will produce a repeating wave of data. [SimpleDataStreamGenerator class](SyntheticExamples/src/main/java/com/google/dataflow/sample/timeseriesflow/examples/simpledata/transforms/SimpleDataStreamGenerator.java) contains the code that will be run. 

The simple data usage is to demonstrate the end to end data engineering of the library. From, time series pre-processing to model creation using TFX.
 
 [SimpleDataStreamGenerator class](SyntheticExamples/src/main/java/com/google/dataflow/sample/timeseriesflow/examples/simpledata/transforms/SimpleDataStreamGenerator.java) has four modes:
 
 * example_1 : In this mode the metrics generated are sent to output as logs.
 * example_2 : In this mode the metrics generated are sent to output as TFExamples to a file system.
 * example_3 : In this mode the metrics generated are sent to output as rows to BigQuery
 * example_4 : In this mode the metrics generated are sent to output as Json string to PubSub

## Example 1 : Output of metrics to LOG

This example will spin up a local pipeline using the Direct Runner, the metrics will be output to log files.
   
```
./gradlew run_example --args='--enablePrintMetricsToLogs'
```

You will see several messages about incomplete TSAccumSequences, once there is enough data, which is 5 secs of data, you will see values output to the logs. To stop you can use CTRL-C.

To start the demo with outlier data included in the stream, which is a value outside of the norm every 50 ticks.

```
./gradlew run_example --args='--enablePrintMetricsToLogs --withOutliers=true'
```

## Example 2 : Creation of TF.Example's from a stream with output to files
### Setup directory
The files will need a location to be output to, in the below example we use /tmp/simple-data/
```
mkdir /<your-directory>/simple-data
./gradlew run_example --args='--enablePrintMetricsToLogs --interchangeLocation=/<your-directory>/simple-data/'
```

Check the folder you specified in --interchangeLocation for new files.

To start the demo with outlier data included in the stream, which is a value outside of the norm every 50 ticks.

```
./gradlew run_example --args='--enablePrintMetricsToLogs --interchangeLocation=/<your-directory>/simple-data/ --withOutliers=true'
```
  
## Example 3 : Example Output of metrics to BigQuery

---
***NOTE***
The following examples will use resources on your Google Cloud Platform account, which will incur costs. 
---

* In order to run this example you will need setup a BigQuery [dataset](https://cloud.google.com/bigquery/docs/datasets-intro) that your local environment will send data to.
* You will need to be authenticated as a user account to run this sample [BigQuery Authentication](https://cloud.google.com/bigquery/docs/authentication)

In the command below, replace <project> with your projectid and give a table prefix which the code will use when creating the table. 


```
./gradlew run_example --args='--bigQueryTableForTSAccumOutputLocation=<project>:<dataset>.<table_prefix>'
```
You can observe the results by running the following SQL command in BigQuery.
```
SELECT
  lower_window_boundary,
  upper_window_boundary,
  is_gap_fill_value,
  DATA.metric,
  CASE DATA.metric
    WHEN 'FIRST_TIMESTAMP' THEN CAST(TIMESTAMP_MILLIS(DATA.lng_data) AS STRING )
    WHEN 'LAST_TIMESTAMP' THEN CAST(TIMESTAMP_MILLIS(DATA.lng_data) AS STRING)
  ELSE
  CAST(DATA.dbl_data AS STRING)
END
FROM
  `<yourtable>`
CROSS JOIN
  UNNEST(DATA) AS DATA
WHERE
  DATA.metric IN ("LAST",
    "FIRST",
    "DATA_POINT_COUNT",
    "FIRST_TIMESTAMP",
    "LAST_TIMESTAMP")
ORDER BY
  lower_window_boundary,
  DATA.metric
```

To start the demo with outlier data included in the stream, which is a value outside of the norm every 50 ticks.

```
./gradlew run_example --args='--bigQueryTableForTSAccumOutputLocation=<project>:<dataset>.<table_prefix> --withOutliers=true'
```

## Example 4 : Sending a stream of examples to PubSub

In this example the values are sent to PubSub and the interchangeLocation. The data sent to PubSub is parsed into JSON format.

```
./gradlew run_example --args='--withOutliers=true --pubSubTopicForTSAccumOutputLocation=projects/<your-project>/topics/outlier-detection'

```