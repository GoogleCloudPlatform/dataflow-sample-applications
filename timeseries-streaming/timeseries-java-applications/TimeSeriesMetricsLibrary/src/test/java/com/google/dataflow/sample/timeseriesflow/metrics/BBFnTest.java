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

import com.google.dataflow.sample.timeseriesflow.FSITechnicalDerivedAggregations.FsiTechnicalIndicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Max;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Min;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Sum;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.bb.BBFn;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.bb.BBFn.BBAvgComputeMethod;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.bb.BBFn.BBOptions;
import com.google.gson.stream.JsonReader;
import common.TSTestData;
import common.TSTestDataBaseline;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;

public class BBFnTest {
  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  /* Simple test to check Exponential Moving Average Technical is created correctly */
  public void testCreateBBSMA() throws IOException {

    String resourceName = "TSTestDataHints.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(5),
                Duration.standardSeconds(15))
            .build();

    BBOptions options = p.getOptions().as(BBOptions.class);
    options.setBBAvgComputeMethod(BBAvgComputeMethod.SIMPLE_MOVING_AVERAGE);
    options.setBBDevFactor(2);

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    PCollection<KV<TSKey, TSAccum>> techAccum =
        p.apply(stream)
            .apply(
                GenerateComputations.builder()
                    .setType1FixedWindow(Duration.standardSeconds(5))
                    .setType2SlidingWindowDuration(Duration.standardSeconds(15))
                    .setEnableGapFill(false)
                    .setBasicType1Metrics(ImmutableList.of(Sum.class, Min.class, Max.class))
                    .setBasicType2Metrics(ImmutableList.of(BBFn.class))
                    .build());

    // The sliding window will create partial values, to keep testing simple we just test
    // correctness of BB for the full value

    PCollection<KV<TSKey, TSAccum>> fullAccum =
        techAccum.apply(
            Filter.by(
                x ->
                    x.getValue()
                            .getDataStoreOrThrow(FsiTechnicalIndicators.SUM_MOVEMENT_COUNT.name())
                            .getIntVal()
                        == 3));

    PCollection<KV<TSKey, List<Double>>> bollingerBand =
        fullAccum.apply(
            "BB",
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(TSKey.class),
                        TypeDescriptors.lists(TypeDescriptors.doubles())))
                .via(
                    x ->
                        KV.of(
                            x.getKey(),
                            Arrays.asList(
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.BB_MIDDLE_BAND_SMA.name())
                                    .getDoubleVal(),
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.BB_UPPER_BAND_SMA.name())
                                    .getDoubleVal(),
                                x.getValue()
                                    .getDataStoreOrThrow(
                                        FsiTechnicalIndicators.BB_BOTTOM_BAND_SMA.name())
                                    .getDoubleVal()))));

    /*
    BB MIDDLE_BAND = SMA or EMA, UPPER_BAND = MIDDLE_BAND + dev_factor * STDDEV, BOTTOM_BAND = MIDDLE_BAND - dev_factor * STDDEV
    BB SMA Key_A_A = [1, 3, 8] = [4.0, 9.8878405776, -1.8878405776]
    BB SMA Key_A_B = [16, 12, 8] = [12.0, 18.5319726474, 5.4680273526]
    BB SMA Key_A_C = [12, 12, 12] = [12, 12, 12]

     */
    PAssert.that(bollingerBand)
        .containsInAnyOrder(
            KV.of(TSTestDataBaseline.KEY_A_A, Arrays.asList(4.0D, 9.8878405776, -1.8878405776D)),
            KV.of(TSTestDataBaseline.KEY_A_B, Arrays.asList(12.0D, 18.5319726474D, 5.4680273526D)),
            KV.of(TSTestDataBaseline.KEY_A_C, Arrays.asList(12D, 12D, 12D)));

    p.run();
  }
}
