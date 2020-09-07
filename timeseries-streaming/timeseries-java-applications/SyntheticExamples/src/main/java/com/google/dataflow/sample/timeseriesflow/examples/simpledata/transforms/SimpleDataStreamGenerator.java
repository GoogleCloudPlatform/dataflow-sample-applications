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

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.AllComputationsExamplePipeline;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.OutPutTFExampleFromTSSequence;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.AllMetricsGeneratorWithDefaults;
import com.google.dataflow.sample.timeseriesflow.transforms.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import com.google.protobuf.util.Timestamps;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.tensorflow.example.Example;

/**
 * This simple example data is used only to demonstrate the end to end data engineering of the
 * library from, timeseries pre-processing to model creation using TFX.
 *
 * <p>The generated data will predictably rise and fall with time.
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
public class SimpleDataStreamGenerator {

  public static void main(String[] args) {

    /**
     * ***********************************************************************************************************
     * We hard code a few of the options for this sample application.
     * ***********************************************************************************************************
     */
    SimpleDataOptions options = PipelineOptionsFactory.fromArgs(args).as(SimpleDataOptions.class);

    Preconditions.checkArgument(
        ImmutableList.of("example_1", "example_2", "example_3", "example_4")
            .contains(Optional.ofNullable(options.getDemoMode()).orElse("")),
        "--demoMode must be set to one of example_1, example_2, example_3, example_4");

    options.setAppName("SimpleDataStreamTSDataPoints");
    options.setTypeOneComputationsLengthInSecs(1);
    options.setTypeTwoComputationsLengthInSecs(5);
    options.setSequenceLengthInSeconds(5);

    Pipeline p = Pipeline.create(options);

    /**
     * ***********************************************************************************************************
     * We want to ensure that there is always a value within each timestep. This is redundant for
     * this dataset as the generated data will always have a value. But we keep this configuration
     * to ensure consistency across the sample pipelines.
     * ***********************************************************************************************************
     */
    PerfectRectangles perfectRectangles =
        PerfectRectangles.builder()
            .setEnableHoldAndPropogate(false)
            .setFixedWindow(Duration.standardSeconds(1))
            .setTtlDuration(Duration.standardSeconds(5))
            .build();

    /**
     * ***********************************************************************************************************
     * The data has only one key, to allow the type 1 computations to be done in parallel we set the
     * {@link GenerateComputations#hotKeyFanOut()}
     * ***********************************************************************************************************
     */
    GenerateComputations generateComputations =
        AllMetricsGeneratorWithDefaults.getGenerateComputationsWithAllKnownMetrics()
            .setType1FixedWindow(Duration.standardSeconds(1))
            .setType2SlidingWindowDuration(Duration.standardSeconds(5))
            .setHotKeyFanOut(5)
            .setPerfectRectangles(perfectRectangles)
            .build();

    /**
     * ***********************************************************************************************************
     * Generate trivial data points that follow a very simple pattern. Over 12 hours the value will
     * cycle through 0 up and then back to 0. There will be a tick every 500 ms.
     * ***********************************************************************************************************
     */
    TSKey key = TSKey.newBuilder().setMajorKey("timeseries_x").setMinorKeyString("value").build();

    boolean outlierEnabled = Optional.ofNullable(options.getWithOutliers()).orElse(false);

    PCollection<TSDataPoint> stream =
        p.apply(GenerateSequence.from(0).withRate(1, Duration.millis(500)))
            .apply(
                ParDo.of(
                    new DoFn<Long, TSDataPoint>() {
                      @ProcessElement
                      public void process(
                          @Element Long input,
                          @Timestamp Instant now,
                          OutputReceiver<TSDataPoint> o) {
                        // We use both 50 and 51 so LAST and FIRST values can become outliers
                        boolean outlier = (outlierEnabled && (input % 50 == 0 || input % 51 == 0));

                        if (outlier) {
                          System.out.println(String.format("Outlier generated at %s", now));
                          o.output(
                              TSDataPoint.newBuilder()
                                  .setKey(key)
                                  .setData(
                                      Data.newBuilder()
                                          .setDoubleVal(
                                              ThreadLocalRandom.current().nextDouble(105D, 200D)))
                                  .setTimestamp(Timestamps.fromMillis(now.getMillis()))
                                  // This metadata is not reported in this version of the sample.
                                  .putMetadata("Bad Data", "YES!")
                                  .build());

                        } else {
                          o.output(
                              TSDataPoint.newBuilder()
                                  .setKey(key)
                                  .setData(
                                      Data.newBuilder()
                                          .setDoubleVal(
                                              Math.round(
                                                      Math.sin(Math.toRadians(input % 360))
                                                          * 10000D)
                                                  / 100D))
                                  .setTimestamp(Timestamps.fromMillis(now.getMillis()))
                                  .build());
                        }
                      }
                    }));

    /**
     * ***********************************************************************************************************
     * All the metrics currently available will be processed for this dataset. The results will be
     * sent to two difference locations: To Google BigQuery as defined by: {@link
     * ExampleTimeseriesPipelineOptions#getBigQueryTableForTSAccumOutputLocation()} To a Google
     * Cloud Storage Bucket as defined by: {@link
     * ExampleTimeseriesPipelineOptions#getInterchangeLocation()} The TFExamples will be grouped
     * into TFRecord files as {@link OutPutTFExampleFromTSSequence#enableSingleWindowFile} is set to
     * false.
     *
     * <p>***********************************************************************************************************
     */
    AllComputationsExamplePipeline allComputationsExamplePipeline =
        AllComputationsExamplePipeline.builder()
            .setTimeseriesSourceName("SimpleExample")
            .setOutputToBigQuery(false)
            .setGenerateComputations(generateComputations)
            .build();

    if (options.getDemoMode().equals("example_3")) {
      allComputationsExamplePipeline =
          allComputationsExamplePipeline.toBuilder().setOutputToBigQuery(true).build();
    }

    PCollection<Iterable<TSAccumSequence>> metrics = stream.apply(allComputationsExamplePipeline);

    /**
     * ***********************************************************************************************************
     * Example_1
     *
     * <p>For demo purposes the values are also sent out to LOG.
     *
     * <p>***********************************************************************************************************
     */
    if (options.getDemoMode().equals("example_1")) {
      metrics.apply(new Print<>());
    }

    /**
     * ***********************************************************************************************************
     * Example 2:
     *
     * <p>Files will be output to the file location specified.
     *
     * <p>Note : For processing of bootstrap history, do not use {@link
     * OutPutTFExampleFromTSSequence#withEnabledSingeWindowFile(boolean)} set to true. This causes
     * output file per type 1 window, which is inefficent when processing history of file, but fine
     * in stream mode.
     *
     * <p>***********************************************************************************************************
     */
    if (options.getDemoMode().equals("example_2")) {
      Preconditions.checkNotNull(
          options.getInterchangeLocation(), "For example_2 you must provide --interchangeLocation");
      System.out.println(
          String.format(
              "Running Example 2, files will be output to %s", options.getInterchangeLocation()));
      metrics
          .apply(
              OutPutTFExampleFromTSSequence.create()
                  .withEnabledSingeWindowFile(true)
                  .setNumShards(2))
          .apply(new Print<Example>());
    }

    if (options.getDemoMode().equals("example_4")) {
      Preconditions.checkNotNull(
          options.getPubSubTopicForTSAccumOutputLocation(),
          "For this example you must set the --pubSubTopicForTSAccumOutputLocation option");
      System.out.println(
          String.format(
              "Running Example 4, protos will be output to %s",
              options.getPubSubTopicForTSAccumOutputLocation()));
      metrics.apply(
          OutPutTFExampleFromTSSequence.create()
              .withPubSubTopic(options.getPubSubTopicForTSAccumOutputLocation())
              .setNumShards(2));
    }

    p.run();
  }
}
