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
import com.google.dataflow.sample.timeseriesflow.io.tfexample.TSAccumIterableToTFExample;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma.MAFn.MAAvgComputeMethod;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.MajorKeyWindowSnapshot;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
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
    Instant now = Instant.parse("2000-01-01T00:00:00Z");

    List<TimestampedValue<TSDataPoint>> data = new ArrayList<>();

    TSKey key = TSKey.newBuilder().setMajorKey("timeseries_x").setMinorKeyString("value").build();

    /*
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
                              Math.round(Math.sin(Math.toRadians(i % 360)) * 10000D) / 100D))
                  .setTimestamp(
                      Timestamps.fromMillis(now.plus(Duration.millis(i * 500)).getMillis()))
                  .build(),
              now.plus(Duration.millis(i * 500))));
    }

    SinWaveExampleOptions options =
        PipelineOptionsFactory.fromArgs(args).as(SinWaveExampleOptions.class);

    /*
     * ***********************************************************************************************************
     * We hard code a few of the options for this sample application.
     * ***********************************************************************************************************
     */
    options.setAppName("SimpleDataBootstrapProcessTSDataPoints");

    options.setRunner(DataflowRunner.class);
    options.setMaxNumWorkers(2);

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
    options.setAbsoluteStopTimeMSTimestamp(now.plus(Duration.standardSeconds(43200)).getMillis());

    // Setup the metrics that are to be computed

    options.setTypeOneBasicMetrics(ImmutableList.of("typeone.Sum", "typeone.Min", "typeone.Max"));
    options.setTypeTwoBasicMetrics(
        ImmutableList.of("typetwo.basic.ma.MAFn", "typetwo.basic.stddev.StdDevFn"));
    options.setTypeTwoComplexMetrics(ImmutableList.of("complex.fsi.rsi.RSIGFn"));

    options.setMAAvgComputeMethod(MAAvgComputeMethod.SIMPLE_MOVING_AVERAGE);

    // Output location for the TFRecord files

    if (options.getInterchangeLocation() == null) {
      options.setInterchangeLocation("<Enter Output location>");
    }

    Pipeline p = Pipeline.create(options);

    /*
     * ***********************************************************************************************************
     * All the metrics currently available will be processed for this dataset. The results will be
     * sent to one location:
     *
     * To a Google Cloud Storage
     * Bucket as defined by: TSOutputPipelineOptions#getInterchangeLocation() The TFExamples
     * will be grouped into TFRecord files as OutPutTFExampleFromTSSequence#enableSingleWindowFile is set to false.
     * ***********************************************************************************************************
     */

    // The data has only one key, to allow the type 1 computations to be done in parallel we set the
    // GenerateComputations#getHotKeyFanOut()

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(options).setHotKeyFanOut(5).build();

    PCollection<KV<TSKey, TSAccum>> computations =
        p.apply(Create.timestamped(data)).apply(generateComputations);

    /*
     * ***********************************************************************************************************
     *
     * Convert raw metrics per timestep into sequence of multivariate data objects.
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

    metrics
        .apply(new TSAccumIterableToTFExample())
        .apply(OutPutTFExampleToFile.create().withEnabledSingeWindowFile(false));
    p.run();
  }
}
