/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.dataflow.sample.timeseriesflow.examples.simpledata.transforms;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.OutPutTFExampleToFile;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.OutPutTFExampleToPubSub;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.TSAccumIterableToTFExample;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma.MAFn.MAAvgComputeMethod;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.MajorKeyWindowSnapshot;
import com.google.dataflow.sample.timeseriesflow.transforms.TSAccumToRow;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.tensorflow.example.Example;

/**
 * This simple example data is used only to demonstrate the end to end data engineering of the
 * library from, timeseries pre-processing to model creation using TFX.
 *
 * <p>The generated data will predictably rise and fall with time. It generate trivial data points
 * that follow a very simple pattern. Over 12 hours the value will cycle through 0 up and then back
 * to 0. There will be a tick every 500 ms.
 *
 * <p>This demo has three modes:
 *
 * <p>example_1: In this mode the metrics are generated and output as logs.
 *
 * <p>example_2: In this mode the metrics are generated and output to the file system.
 *
 * <p>example_3: In this mode the metrics are generated and output to BigQuery.
 *
 * <p>example_4: In this mode the metrics are generated and output to PubSub.
 */
public class SinWaveExample {

  public static void main(String[] args) {

    /*
     * ***********************************************************************************************************
     * We hard code a few of the options for this sample application to make the sample easier to
     * follow. Normally all options would be passed in via args in format --optionName=value
     * ***********************************************************************************************************
     */
    SinWaveExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SinWaveExampleOptions.class);

    options.setAppName("SimpleDataStreamTSDataPoints");

    // Type 1 time length and Type 2 time length
    options.setTypeOneComputationsLengthInSecs(1);
    options.setTypeTwoComputationsLengthInSecs(5);

    // How many timesteps to output
    options.setOutputTimestepLengthInSecs(5);

    // Parameters for gap filling.
    // We want to ensure that there is always a value within each timestep. This is redundant for
    // this dataset as the generated data will always have a value. But we keep this configuration
    // to ensure consistency across the sample pipelines.

    options.setGapFillEnabled(true);
    options.setEnableHoldAndPropogateLastValue(false);
    options.setTTLDurationSecs(1);

    // Setup the metrics that are to be computed

    options.setTypeOneBasicMetrics(ImmutableList.of("typeone.Sum", "typeone.Min", "typeone.Max"));
    options.setTypeTwoBasicMetrics(
        ImmutableList.of("typetwo.basic.ma.MAFn", "typetwo.basic.stddev.StdDevFn"));
    options.setTypeTwoComplexMetrics(ImmutableList.of("complex.fsi.rsi.RSIGFn"));

    options.setMAAvgComputeMethod(MAAvgComputeMethod.SIMPLE_MOVING_AVERAGE);

    Pipeline p = Pipeline.create(options);

    TSKey key = TSKey.newBuilder().setMajorKey("timeseries_x").setMinorKeyString("value").build();

    boolean outlierEnabled = Optional.ofNullable(options.getWithOutliers()).orElse(false);

    PCollection<TSDataPoint> stream = getSyntheticStream(p, outlierEnabled, key);

    /*
     * ***********************************************************************************************************
     *
     * All the configuration for generating computations is passed from options. This transform
     * is the core of the sample library and generates Type 1 and Type 2 computations at every fixed
     * window offset.
     *
     * The results will be sent to three difference locations dependent on the options chosen:
     *
     * 1
     * --
     * To LOG output as defined in this examples options (note this option is not available in the core library):
     * ExampleTimeseriesPipelineOptions#getEnablePrintMetricsToLogs()
     *
     * 2
     * --
     * To Google BigQuery as defined by:
     * ExampleTimeseriesPipelineOptions#getBigQueryTableForTSAccumOutputLocation()
     *
     * 3
     * --
     * To a Google Cloud Storage Bucket as defined by:
     * ExampleTimeseriesPipelineOptions#getInterchangeLocation()
     *
     * ***********************************************************************************************************
     */

    /*
     * ***********************************************************************************************************
     *
     * Stage 1 create Computations
     *
     * ***********************************************************************************************************
     */

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(options).build();

    PCollection<KV<TSKey, TSAccum>> computations = stream.apply(generateComputations);

    /*
     * ***********************************************************************************************************
     *
     * Stage 2 output results to BigQuery if requested
     *
     * ***********************************************************************************************************
     */

    DateTimeFormatter formatter = DateTimeFormat.forPattern("YYYY_MM_dd_HH_mm_ss");

    if (options.getBigQueryTableForTSAccumOutputLocation() != null) {
      computations
          .apply(Values.create())
          .apply(new TSAccumToRow())
          .apply(
              BigQueryIO.<Row>write()
                  .useBeamSchema()
                  .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                  .to(
                      String.join(
                          "_",
                          options.getBigQueryTableForTSAccumOutputLocation(),
                          Optional.of("SimpleExample").orElse(""),
                          Instant.now().toString(formatter))));
    }

    /*
     * ***********************************************************************************************************
     *
     * Stage 3 Convert raw metrics per timestep into sequence of multivariate data objects.
     *
     * ***********************************************************************************************************
     */

    /*
     * ***********************************************************************************************************
     *
     * Now that we have our metrics for every time offset, create a sequence which contains an ordered set of
     * data points from now - the OutputTimestepLength. As we set setOutputTimestepLengthInSecs to 5 and our Type 1
     * fixed window is 1 sec this results in a structure of [Timesteps,1] where timesteps == 5.
     *
     * getNumSequenceTimesteps is being used as in this example we do not 'bootstrap' the pipeline.
     * So on startup the first few outputs will not have timesteps=5, so we prevent these values from being output
     * using this variable.
     *
     * ***********************************************************************************************************
     */

    PCollection<KV<TSKey, TSAccumSequence>> sequences =
        computations.apply(
            ConvertAccumToSequence.builder()
                .setCheckHasValueCountOf(CommonUtils.getNumOfSequenceTimesteps(options))
                .build());

    /*
     * ***********************************************************************************************************
     *
     * Create a window snapshot of the all the values.
     * In this step we convert from the structure of [Timestep,1] to [Timestep,Metrics] where all
     * keys and features are brought together into a single structure.
     *
     * ***********************************************************************************************************
     */

    PCollection<Iterable<TSAccumSequence>> metrics =
        sequences.apply(MajorKeyWindowSnapshot.generateWindowSnapshot());

    /*
     * ***********************************************************************************************************
     *
     * If print metrics is enabled, output all the type 1 and type 2 metrics that have been
     * generated.
     *
     * ***********************************************************************************************************
     */
    if (options.getEnablePrintMetricsToLogs()) {
      metrics.apply(new Print<>());
    }

    /*
     * ***********************************************************************************************************
     *
     * Files will be output to the file location specified.
     *
     * Note : For processing of historical data in batch mode, do not have
     * OutPutTFExampleFromTSSequence#withEnabledSingeWindowFile(boolean) set to true. This causes
     * one output file per type 1 window, which is inefficient.
     *
     * The TFExamples will be grouped into TFRecord files as OutPutTFExampleFromTSSequence#enableSingleWindowFile
     * is set to false.
     *
     * ***********************************************************************************************************
     */
    if (options.getInterchangeLocation() != null) {
      PCollection<Example> examples = metrics.apply(new TSAccumIterableToTFExample());
      System.out.println(
          String.format("Running Example , with output to %s", options.getInterchangeLocation()));
      examples.apply(OutPutTFExampleToFile.create());

      if (options.getEnablePrintTFExamplesToLogs()) {
        examples.apply(new Print<>());
      }
    }

    /*
     * ***********************************************************************************************************
     *
     * TF.Example protos will be sent to PubSub as JSON if the PubSub output location is
     * provided.
     *
     * ***********************************************************************************************************
     */
    if (options.getPubSubTopicForTSAccumOutputLocation() != null) {
      PCollection<Example> examples = metrics.apply(new TSAccumIterableToTFExample());
      System.out.println(
          String.format(
              "Running Example , with output to %s",
              options.getPubSubTopicForTSAccumOutputLocation()));
      examples.apply(
          OutPutTFExampleToPubSub.create(options.getPubSubTopicForTSAccumOutputLocation()));

      if (options.getEnablePrintTFExamplesToLogs()) {
        examples.apply(new Print<>());
      }
    }

    p.run();
  }

  /**
   * ***********************************************************************************************************
   * Generate trivial data points that follow a very simple pattern. Over 12 hours the value will
   * cycle through 0 up and then back to 0. There will be a tick every 500 ms.
   * ***********************************************************************************************************
   */
  private static PCollection<TSDataPoint> getSyntheticStream(
      Pipeline p, boolean outlierEnabled, TSKey key) {
    return p.apply(GenerateSequence.from(0).withRate(1, Duration.millis(500)))
        .apply(
            ParDo.of(
                new DoFn<Long, TSDataPoint>() {
                  @ProcessElement
                  public void process(
                      @Element Long input, @Timestamp Instant now, OutputReceiver<TSDataPoint> o) {
                    // We use both 50 and 51 so LAST and FIRST values can become outliers
                    boolean outlier = (outlierEnabled && (input % 50 == 0 || input % 51 == 0));

                    Instant startOfCycle = Instant.parse("2000-01-01T00:00:00Z");
                    long offset = (now.getMillis() - startOfCycle.getMillis()) / 500L;

                    if (outlier) {
                      System.out.println(String.format("Outlier generated at %s", now));
                      o.outputWithTimestamp(
                          TSDataPoint.newBuilder()
                              .setKey(key)
                              .setData(
                                  Data.newBuilder()
                                      .setDoubleVal(
                                          ThreadLocalRandom.current().nextDouble(105D, 200D)))
                              .setTimestamp(Timestamps.fromMillis(now.getMillis()))
                              // This metadata is not reported in this version of the sample.
                              .putMetadata("Bad Data", "YES!")
                              .build(),
                          now);

                    } else {

                      // Round to the nearest ceil 500 ms
                      long timestamp = (long) (Math.ceil((double) now.getMillis() / 500) * 500);

                      o.outputWithTimestamp(
                          TSDataPoint.newBuilder()
                              .setKey(key)
                              .setData(
                                  Data.newBuilder()
                                      .setDoubleVal(
                                          Math.round(
                                                  Math.sin(Math.toRadians(offset % 360)) * 10000D)
                                              / 100D))
                              // Round to nearest 500 ms
                              .setTimestamp(Timestamps.fromMillis(timestamp))
                              .build(),
                          Instant.ofEpochMilli(timestamp));
                    }
                  }
                }));
  }
}
