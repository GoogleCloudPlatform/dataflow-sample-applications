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

import com.google.common.collect.ImmutableSet;
import com.google.dataflow.sample.timeseriesflow.AllComputationsExamplePipeline;
import com.google.dataflow.sample.timeseriesflow.ExampleTimeseriesPipelineOptions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.OutPutTFExampleToFile;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.OutPutTFExampleToPubSub;
import com.google.dataflow.sample.timeseriesflow.io.tfexample.TSAccumIterableToTFExample;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.AllMetricsWithDefaults;
import com.google.dataflow.sample.timeseriesflow.transforms.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import java.io.File;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import com.google.dataflow.sample.timeseriesflow.transforms.TSAccumToJson;
import com.google.dataflow.sample.timeseriesflow.transforms.TSAccumToRow;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.*;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.tensorflow.example.Example;
import org.tensorflow.op.core.Print;

import static java.lang.Boolean.TRUE;

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


    LocalDateTime ldt = LocalDateTime.parse("2020-05-12T00:00:00");
    String timezone = options.getTimezone();
    if (timezone == null) {
      throw new IllegalArgumentException("Please specify timezone parameter");
    }
    ZonedDateTime zdt = ldt.atZone(ZoneId.of(timezone));

    long millis = zdt.toInstant().toEpochMilli();

    options.setTypeOneComputationsLengthInSecs(options.getResampleSec());
    options.setTypeTwoComputationsLengthInSecs(options.getWindowSec());
    options.setSequenceLengthInSeconds(options.getWindowSec());

    options.setAbsoluteStopTimeMSTimestamp(millis);
    options.setEnableHoldAndPropogate(TRUE);
    // We fill gaps for 10 min during quiet periods of forex markets
    options.setTTLDurationSecs(600);

    Pipeline p = Pipeline.create(options);

//    String resourceName = "EURUSD-2020-05-11_2020-05-11.csv";
//
//    ClassLoader classLoader = ForexBatchPipeline.class.getClassLoader();
//    File file = null;
//    try {
//      file = new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//    String absolutePath = null;
//    try {
//      absolutePath = file.getAbsolutePath();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }

    String absolutePath = "gs://4f93039d-7a71-48e7-800b-59f642741ebd/MarketData/FX/EURUSD/tick/EURUSD-2020-05-11_2020-05-11.csv";

    /**
     * ***********************************************************************************************************
     * The data has only one key, to allow the type 1 computations to be done in parallel we set the
     * {@link GenerateComputations#hotKeyFanOut()}
     * ***********************************************************************************************************
     */
    GenerateComputations.Builder generateComputations = null;
    if (options.getMetrics() == null) {
      generateComputations =
              GenerateComputations.fromPiplineOptions(options)
                      .setType1NumericComputations(AllMetricsWithDefaults.getAllType1Combiners())
                      .setType2NumericComputations(AllMetricsWithDefaults.getAllType2Computations());
    };

    /**
     * ***********************************************************************************************************
     * We want to ensure that there is always a value within each timestep. This is redundant for
     * this dataset as the generated data will always have a value. But we keep this configuration
     * to ensure consistency across the sample pipelines.
     * ***********************************************************************************************************
     */
    generateComputations.setPerfectRectangles(PerfectRectangles.fromPipelineOptions(options));


    p
            .apply(
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
