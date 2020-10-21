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
package com.google.dataflow.sample.timeseriesflow.adaptors.domain.tests;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.adaptors.domain.BlendedIndex;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.test.TSDataTestUtils;
import com.google.gson.stream.JsonReader;
import com.google.protobuf.util.Timestamps;
import common.TSTestData;
import java.io.File;
import java.io.FileReader;
import java.util.Map;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableMap;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableSet;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class BlendedIndexTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final Instant NOW = Instant.parse("2000-01-01T00:00:00Z");

  /**
   * Key A [ 10, 20 , 30 ] Key B [ 110, 120 , 130 ] Key C [ 210, 220 , 230 ] Rations
   * [0.25,0.25,0.50]
   *
   * <p>Expected output [10*.25+110*.25+210*.50] = 132.50 [20*.25+120*.25+220*.50] = 142.50
   * [30*.25+130*.25+230*.50] = 152.50
   */
  @Test
  public void testNonGapData() throws Exception {

    String resourceName = "TSTestBlendedIndexNoGaps.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(1),
                Duration.standardSeconds(1))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    Map<TSKey, Double> map =
        ImmutableMap.of(
            TSDataTestUtils.KEY_A_A,
            0.25D,
            TSDataTestUtils.KEY_B_A,
            0.25D,
            TSDataTestUtils.KEY_C_A,
            0.5D);

    PCollectionView<Map<TSKey, Double>> ratios = p.apply(Create.of(map)).apply(View.asMap());

    TSKey newKey = TSKey.newBuilder().setMajorKey("Index").setMinorKeyString("value").build();

    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<TSDataPoint> result =
        testStream.apply(
            BlendedIndex.builder()
                .setIndexList(
                    ImmutableSet.of(
                        TSDataTestUtils.KEY_A_A, TSDataTestUtils.KEY_B_A, TSDataTestUtils.KEY_C_A))
                .setRatioList(ratios)
                .setIndexKey(newKey)
                .setTimeToLive(Duration.millis(0))
                .setDownSampleWindowLength(Duration.standardSeconds(1))
                .build());

    TSDataPoint first =
        TSDataPoint.newBuilder()
            .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 999))
            .setData(CommonUtils.createNumData((10 * .25 + 110 * .25 + 210 * 0.50)))
            .setKey(newKey)
            .build();

    PAssert.that(result)
        .containsInAnyOrder(
            first,
            first
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1999))
                .setData(CommonUtils.createNumData((20 * .25 + 120 * .25 + 220 * 0.50)))
                .build(),
            first
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setData(CommonUtils.createNumData((30 * .25 + 130 * .25 + 230 * 0.50)))
                .build());

    p.run();
  }

  @Test
  public void testGapData() throws Exception {

    String resourceName = "TSTestBlendedIndexWithGaps.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(1),
                Duration.standardSeconds(1))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    Map<TSKey, Double> map =
        ImmutableMap.of(
            TSDataTestUtils.KEY_A_A,
            0.25D,
            TSDataTestUtils.KEY_B_A,
            0.25D,
            TSDataTestUtils.KEY_C_A,
            0.5D);

    PCollectionView<Map<TSKey, Double>> ratios = p.apply(Create.of(map)).apply(View.asMap());

    TSKey newKey = TSKey.newBuilder().setMajorKey("Index").setMinorKeyString("value").build();

    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<TSDataPoint> result =
        testStream.apply(
            BlendedIndex.builder()
                .setIndexList(
                    ImmutableSet.of(
                        TSDataTestUtils.KEY_A_A, TSDataTestUtils.KEY_B_A, TSDataTestUtils.KEY_C_A))
                .setRatioList(ratios)
                .setIndexKey(newKey)
                .setAbsoluteStopTime(Instant.ofEpochMilli(TSDataTestUtils.START + 3000))
                .setDownSampleWindowLength(Duration.standardSeconds(1))
                .build());

    TSDataPoint first =
        TSDataPoint.newBuilder()
            .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 999))
            .setData(CommonUtils.createNumData((10 * .25 + 110 * .25 + 210 * 0.50)))
            .setKey(newKey)
            .build();

    PAssert.that(result)
        .containsInAnyOrder(
            first,
            first
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 1999))
                .setData(CommonUtils.createNumData((20 * .25 + 110 * .25 + 220 * 0.50)))
                .build(),
            first
                .toBuilder()
                .setTimestamp(Timestamps.fromMillis(TSDataTestUtils.START + 2999))
                .setData(CommonUtils.createNumData((30 * .25 + 130 * .25 + 220 * 0.50)))
                .build());

    p.run();
  }

  @Test
  /** Test to ensure no data is output when the full input key list is not satisfied */
  public void testMissingBootstrapValue() throws Exception {

    String resourceName = "TSTestBlendedIndexNoGaps.json";
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(classLoader.getResource(resourceName).getFile());
    String absolutePath = file.getAbsolutePath();

    TSTestData tsTestData =
        TSTestData.toBuilder()
            .setInputTSDataFromJSON(
                new JsonReader(new FileReader(absolutePath)),
                Duration.standardSeconds(1),
                Duration.standardSeconds(1))
            .build();

    TestStream<TSDataPoint> stream = tsTestData.inputTSData();

    Map<TSKey, Double> map =
        ImmutableMap.of(
            TSDataTestUtils.KEY_A_A,
            0.25D,
            TSDataTestUtils.KEY_B_A,
            0.25D,
            TSDataTestUtils.KEY_C_A,
            0.5D,
            TSDataTestUtils.KEY_A_B,
            0.5D);

    PCollectionView<Map<TSKey, Double>> ratios = p.apply(Create.of(map)).apply(View.asMap());

    TSKey newKey = TSKey.newBuilder().setMajorKey("Index").setMinorKeyString("value").build();

    PCollection<TSDataPoint> testStream = p.apply(stream);

    PCollection<TSDataPoint> result =
        testStream.apply(
            BlendedIndex.builder()
                .setIndexList(
                    ImmutableSet.of(
                        TSDataTestUtils.KEY_A_A,
                        TSDataTestUtils.KEY_B_A,
                        TSDataTestUtils.KEY_C_A,
                        TSDataTestUtils.KEY_A_B))
                .setRatioList(ratios)
                .setIndexKey(newKey)
                .setAbsoluteStopTime(Instant.ofEpochMilli(TSDataTestUtils.START + 3000))
                .setDownSampleWindowLength(Duration.standardSeconds(1))
                .build());

    PAssert.that(result).empty();
    p.run();
  }
}
