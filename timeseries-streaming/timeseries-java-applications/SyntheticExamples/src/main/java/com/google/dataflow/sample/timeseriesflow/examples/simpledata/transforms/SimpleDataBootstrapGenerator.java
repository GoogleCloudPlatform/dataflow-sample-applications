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
import com.google.dataflow.sample.timeseriesflow.ExampleTimeseriesPipelineOptions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.OutPutTFExampleFromTSSequence;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.AllMetricsGeneratorWithDefaults;
import com.google.dataflow.sample.timeseriesflow.transforms.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * This trivial example data is used only to demonstrate the end to end data engineering of the
 * library from, timeseries pre-processing to model creation using TFX.
 *
 * <p>The learning bootstrap data will predictably rise and fall with time.
 *
 * <p>Options that are required for the pipeline:
 */
public class SimpleDataBootstrapGenerator {

  public static void main(String[] args) {

    // The starting time for the bootstrap data is unimportant for this dataset as the data function
    // is always the same.
    Instant now = Instant.parse("2000-01-01T00:00:00");

    List<TimestampedValue<TSDataPoint>> data = new ArrayList<>();

    TSKey key = TSKey.newBuilder().setMajorKey("timeseries_x").setMinorKeyString("value").build();

    /**
     * ***********************************************************************************************************
     * Generate trivial data points that follow a very simple pattern. Over 12 hours the value will
     * cycle through 0 to x and back to 0. There will be a tick every 500 ms.
     * ***********************************************************************************************************
     */
    for (int i = 0; i < 86400; i++) {
      data.add(
          TimestampedValue.of(
              TSDataPoint.newBuilder()
                  .setKey(key)
                  .setData(
                      Data.newBuilder()
                          .setDoubleVal(
                              Math.round(((Math.sin(Math.toRadians(i % 360)) * 100) + 100) * 100.0)
                                  / 100.0))
                  .setTimestamp(
                      Timestamps.fromMillis(now.plus(Duration.millis(i * 500)).getMillis()))
                  .build(),
              now.plus(Duration.millis(i * 500))));
    }

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

    ExampleTimeseriesPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(ExampleTimeseriesPipelineOptions.class);

    /**
     * ***********************************************************************************************************
     * We hard code a few of the options for this sample application.
     * ***********************************************************************************************************
     */

    options.setAppName("SimpleDataBootstrapProcessTSDataPoints");
    options.setTypeOneComputationsLengthInSecs(1);
    options.setTypeTwoComputationsLengthInSecs(5);
    options.setSequenceLengthInSeconds(5);
    options.setRunner(DataflowRunner.class);
    options.setMaxNumWorkers(2);

    Pipeline p = Pipeline.create(options);

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
    p.apply(Create.timestamped(data))
        .apply(
            AllComputationsExamplePipeline.builder()
                .setTimeseriesSourceName("SimpleExample")
                .setOutputToBigQuery(false)
                .setGenerateComputations(generateComputations)
                .build())
        .apply(OutPutTFExampleFromTSSequence.create().withEnabledSingeWindowFile(false));

    p.run();
  }
}
