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
package com.google.dataflow.sample.timeseriesflow.test;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.test.SnapShotUtils.IterableTSAccumSequenceTSKeyDoFn;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnapShotScalabilityUtilsTest {

  static ScaleTestingOptions OPTIONS = PipelineOptionsFactory.as(ScaleTestingOptions.class);

  {
    OPTIONS.setAppName("SimpleDataStreamTSDataPoints");
    OPTIONS.setTypeOneComputationsLengthInSecs(1);
    OPTIONS.setTypeTwoComputationsLengthInSecs(3);
    OPTIONS.setOutputTimestepLengthInSecs(3);
    OPTIONS.setNumKeys(1);
    OPTIONS.setNumSecs(3);
    OPTIONS.setNumFeatures(1);
  }

  @Rule public final transient TestPipeline p = TestPipeline.fromOptions(OPTIONS);

  @Test
  public void testSnapShotScalabilityOneKey() {

    PCollection<Iterable<TSAccumSequence>> result = SnapShotUtils.testSnapShotScalability(p);

    PCollection<TSKey> windowKeys = result.apply(ParDo.of(new IterableTSAccumSequenceTSKeyDoFn()));

    windowKeys.apply(Reify.windows()).apply(new Print<>());

    TSKey key =
        TSDataTestUtils.KEY_A_A
            .toBuilder()
            .setMajorKey(TSDataTestUtils.KEY_A_A.getMajorKey().concat("0"))
            .build();

    PAssert.that(windowKeys)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START - 2000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START - 1000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(3)))
        .containsInAnyOrder(key)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 1000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key);

    p.run();
  }

  @Test
  public void testSnapShotScalabilityThreeKey() {

    p.getOptions().as(ScaleTestingOptions.class).setNumKeys(3);

    PCollection<Iterable<TSAccumSequence>> result = SnapShotUtils.testSnapShotScalability(p);

    PCollection<TSKey> windowKeys = result.apply(ParDo.of(new IterableTSAccumSequenceTSKeyDoFn()));

    TSKey key1 =
        TSDataTestUtils.KEY_A_A
            .toBuilder()
            .setMajorKey(TSDataTestUtils.KEY_A_A.getMajorKey().concat("0"))
            .build();

    TSKey key2 =
        TSDataTestUtils.KEY_A_A
            .toBuilder()
            .setMajorKey(TSDataTestUtils.KEY_A_A.getMajorKey().concat("1"))
            .build();

    TSKey key3 =
        TSDataTestUtils.KEY_A_A
            .toBuilder()
            .setMajorKey(TSDataTestUtils.KEY_A_A.getMajorKey().concat("2"))
            .build();

    PAssert.that(windowKeys)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START - 2000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key1, key2, key3)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START - 1000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key1, key2, key3)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(3)))
        .containsInAnyOrder(key1, key2, key3)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 1000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key1, key2, key3)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START + 2000), Duration.standardSeconds(3)))
        .containsInAnyOrder(key1, key2, key3);

    p.run();
  }
}
