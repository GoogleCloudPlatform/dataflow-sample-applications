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
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
 * <p>example_2: In this mode the metrics are generated and output to BigQuery
 *
 * <p>example_1: In this mode the metrics are generated and output as logs.
 */
public class SimpleDataStreamGenerator {

  public static void main(String[] args) {

    /**
     * ***********************************************************************************************************
     * We want to ensure that there is always a value within each timestep. This is redundent for
     * this dataset as the generated data will always have a value. But we keep this configuration
     * to ensure consistency accross the sample pipelines.
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
     * The data has only one key, to allow the type 1 computations to be done in parrallal we set
     * the {@link GenerateComputations#hotKeyFanOut()}
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
     * We hard code a few of the options for this sample application.
     * ***********************************************************************************************************
     */
    SimpleDataOptions options = PipelineOptionsFactory.fromArgs(args).as(SimpleDataOptions.class);

    options.setAppName("SimpleDataStreamTSDataPoints");
    options.setTypeOneComputationsLengthInSecs(1);
    options.setTypeTwoComputationsLengthInSecs(5);
    options.setSequenceLengthInSeconds(5);

    Pipeline p = Pipeline.create(options);

    TSKey key = TSKey.newBuilder().setMajorKey("timeseries_x").setMinorKeyString("value").build();

    /**
     * ***********************************************************************************************************
     * Generate trivial data points that follow a very simple pattern. Over 12 hours the value will
     * cycle through 0 to 120 and back to 0. There will be a tick every 500 ms.
     * ***********************************************************************************************************
     */
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
                        o.output(
                            TSDataPoint.newBuilder()
                                .setKey(key)
                                .setData(
                                    Data.newBuilder()
                                        .setDoubleVal(
                                            Math.round(
                                                    ((Math.sin(Math.toRadians(input % 360)) * 100)
                                                            + 100)
                                                        * 100.0)
                                                / 100.0))
                                .setTimestamp(Timestamps.fromMillis(now.getMillis()))
                                .build());
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

    PCollection<KV<TSKey, Iterable<TSAccumSequence>>> metrics =
        stream.apply(allComputationsExamplePipeline);

    /**
     * ***********************************************************************************************************
     * Example_1
     *
     * <p>For demo purposes the values are also sent out to LOG.
     *
     * <p>***********************************************************************************************************
     */
    if (options.getDemoMode().equals("example_1")) {
      metrics.apply(Values.create()).apply(new Print<>());
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

    if (options.getDemoMode().equals("example_3")) {
      allComputationsExamplePipeline.toBuilder().setOutputToBigQuery(true);
    }

    p.run();
  }
}
