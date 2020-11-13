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
package com.google.dataflow.sample.timeseriesflow.examples.fsi.forex;

import static java.lang.Boolean.TRUE;

import com.google.common.collect.ImmutableSet;
import com.google.dataflow.sample.timeseriesflow.AllComputationsExamplePipeline;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.AllMetricsWithDefaults;
import com.google.dataflow.sample.timeseriesflow.transforms.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import java.time.Instant;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class ForexBatchPipeline {

  public static void main(String[] args) {

    /**
     * ***********************************************************************************************************
     * We hard code a few of the options for this sample application.
     * ***********************************************************************************************************
     */
    ExampleForexPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(ExampleForexPipelineOptions.class);
    options.setAppName("ForexExample");

    // Absolute UTC timestamp to stop filling gaps, e.g., end of time series dataset
    Instant timestamp = Instant.parse(options.getEndTimestamp());
    if (timestamp == null) {
      Instant.parse("2020-05-12T00:00:00"); // Default to example dataset end of day
    }

    long millis = timestamp.toEpochMilli();

    options.setTypeOneComputationsLengthInSecs(options.getResampleSec());
    options.setTypeTwoComputationsLengthInSecs(options.getWindowSec());
    options.setSequenceLengthInSeconds(options.getWindowSec());

    // Absolute time is required for batch jobs to determine when to stop filling gaps in bound
    // datasets
    options.setAbsoluteStopTimeMSTimestamp(millis);
    options.setEnableHoldAndPropogate(TRUE);

    // We fill gaps for 50 hours during quiet periods of forex markets, and including weekend when
    // markets are closed,
    // before reaching the absolute stop time
    options.setTTLDurationSecs(18000);

    Pipeline p = Pipeline.create(options);

    // get absolute path of input dataset, could be local or a cloud bucket
    String absolutePath = options.getInputPath();

    /**
     * ***********************************************************************************************************
     * The data has only one key, to allow the type 1 computations to be done in parallel we set the
     * {@link GenerateComputations#hotKeyFanOut()}
     * ***********************************************************************************************************
     */
    GenerateComputations.Builder generateComputations =
        GenerateComputations.fromPiplineOptions(options)
            .setType1NumericComputations(AllMetricsWithDefaults.getAllType1Combiners())
            .setType2NumericComputations(AllMetricsWithDefaults.getAllType2Computations());

    /**
     * ***********************************************************************************************************
     * We want to ensure that there is always a value within each timestep. This is redundant for
     * this dataset as the generated data will always have a value. But we keep this configuration
     * to ensure consistency across the sample pipelines.
     * ***********************************************************************************************************
     */
    generateComputations.setPerfectRectangles(PerfectRectangles.fromPipelineOptions(options));

    p.apply(
            HistoryForexReader.builder()
                .setSourceFilesURI(absolutePath)
                .setTickers(ImmutableSet.of("EURUSD"))
                .build())
        .apply(
            AllComputationsExamplePipeline.builder()
                .setTimeseriesSourceName("Forex")
                .setGenerateComputations(generateComputations.build())
                .build());

    p.run();
  }
}
