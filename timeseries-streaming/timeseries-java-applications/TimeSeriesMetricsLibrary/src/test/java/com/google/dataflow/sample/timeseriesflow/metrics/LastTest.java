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
package com.google.dataflow.sample.timeseriesflow.metrics;

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Last;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import com.google.gson.stream.JsonReader;
import common.TSTestData;
import java.io.File;
import java.io.FileReader;
import java.util.Optional;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LastTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  private static final String PRICE = "PRICE";

  @Test
  /**
   * This is an integration test which will simulate a real computation type Volume Weighted Average
   * Price
   */
  public void testLast() throws Exception {

    String resourceName = "LastTest.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSFlowOptions options = p.getOptions().as(TSFlowOptions.class);
    options.setTypeOneComputationsLengthInSecs(5);
    options.setTypeTwoComputationsLengthInSecs(5);

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(5))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    GenerateComputations generateComputations =
        GenerateComputations.fromPiplineOptions(options)
            .setBasicType1Metrics(ImmutableList.of(Last.class))
            .build();

    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<String> result =
        testStream
            .apply(generateComputations)
            .apply(Values.create())
            .apply(
                MapElements.into(TypeDescriptors.strings())
                    .via(
                        x ->
                            x.getKey().getMajorKey()
                                + "::"
                                + Optional.ofNullable(
                                        x.getDataStoreMap().get(Indicators.LAST.name()))
                                    .orElse(CommonUtils.createNumData(0D))
                                    .getDoubleVal()));

    PAssert.that(result)
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:00Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::40.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:05Z"), Duration.standardSeconds(5)))
        .containsInAnyOrder("Key-A::10.0")
        .inWindow(
            new IntervalWindow(new Instant("2000-01-01T00:00:10Z"), Duration.standardSeconds(5)))
        .empty();

    p.run();
  }
}
