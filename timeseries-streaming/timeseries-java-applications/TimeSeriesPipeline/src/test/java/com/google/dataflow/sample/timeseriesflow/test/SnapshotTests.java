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

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations.Indicators;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.test.SnapShotUtils.IterableTSAccumSequenceTSKeyDoFn;
import com.google.dataflow.sample.timeseriesflow.test.SnapShotUtils.IterableTSAccumTSKeyDoFn;
import com.google.dataflow.sample.timeseriesflow.transforms.MajorKeyWindowSnapshot;
import com.google.dataflow.sample.timeseriesflow.transforms.MinorKeyWindowSnapshot;
import com.google.protobuf.util.Durations;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SnapshotTests {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  static final TSAccum FIRST_ACCUM_A_A =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_A)
          .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(), CommonUtils.createNumData(TSDataTestUtils.START))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum FIRST_ACCUM_A_B =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_B)
          .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(), CommonUtils.createNumData(TSDataTestUtils.START))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum FIRST_ACCUM_A_C =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_C)
          .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(), CommonUtils.createNumData(TSDataTestUtils.START))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum FIRST_ACCUM_B_A =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_B_A)
          .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(), CommonUtils.createNumData(TSDataTestUtils.START))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum SECOND_ACCUM_A_A =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_A)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_TEN_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum SECOND_ACCUM_A_B =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_B)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_TEN_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum SECOND_ACCUM_A_C =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_C)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_TEN_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum SECOND_ACCUM_B_A =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_B_A)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_TEN_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccum SECOND_ACCUM_B_B =
      TSAccum.newBuilder()
          .setKey(TSDataTestUtils.KEY_B_B)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .putDataStore(
              Indicators.FIRST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_FIVE_SECS))
          .putDataStore(
              Indicators.LAST_TIMESTAMP.name(),
              CommonUtils.createNumData(TSDataTestUtils.PLUS_TEN_SECS))
          .putDataStore(Indicators.MAX.name(), CommonUtils.createNumData(1F))
          .build();

  static final TSAccumSequence FIRST_ACCUM_SEQ_A_A =
      TSAccumSequence.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_A)
          .addAccums(FIRST_ACCUM_A_A)
          .setLowerWindowBoundary(TSDataTestUtils.START_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setDuration(Durations.fromSeconds(10))
          .setCount(1)
          .build();

  static final TSAccumSequence SECOND_ACCUM_SEQ_A_A =
      TSAccumSequence.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_A)
          .addAccums(SECOND_ACCUM_A_A)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .setDuration(Durations.fromSeconds(10))
          .setCount(1)
          .build();

  static final TSAccumSequence SECOND_ACCUM_SEQ_A_B =
      TSAccumSequence.newBuilder()
          .setKey(TSDataTestUtils.KEY_A_B)
          .addAccums(SECOND_ACCUM_A_B)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .setDuration(Durations.fromSeconds(10))
          .setCount(1)
          .build();

  static final TSAccumSequence SECOND_ACCUM_SEQ_B_A =
      TSAccumSequence.newBuilder()
          .setKey(TSDataTestUtils.KEY_B_A)
          .addAccums(SECOND_ACCUM_B_A)
          .setLowerWindowBoundary(TSDataTestUtils.PLUS_FIVE_SECS_TIMESTAMP)
          .setUpperWindowBoundary(TSDataTestUtils.PLUS_TEN_SECS_TIMESTAMP)
          .setDuration(Durations.fromSeconds(10))
          .setCount(1)
          .build();

  @Test
  /**
   * Given Major Keys A and B and minor Keys A and B per window all values should collapse together
   * to become Iterable{A-A, A-B, B-A, B-B}
   */
  public void testFromTSAccumMultipltMajorKeys() {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    TestStream<KV<TSKey, TSAccum>> testStream =
        TestStream.create(CommonUtils.getKvTSAccumCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                KV.of(TSDataTestUtils.KEY_A_A, FIRST_ACCUM_A_A),
                KV.of(TSDataTestUtils.KEY_B_A, FIRST_ACCUM_B_A))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS))
            .addElements(
                KV.of(TSDataTestUtils.KEY_A_A, SECOND_ACCUM_A_A),
                KV.of(TSDataTestUtils.KEY_A_B, SECOND_ACCUM_A_B),
                KV.of(TSDataTestUtils.KEY_B_A, SECOND_ACCUM_B_A),
                KV.of(TSDataTestUtils.KEY_B_B, SECOND_ACCUM_B_B))
            .advanceWatermarkToInfinity();

    PCollection<Iterable<TSAccum>> result =
        p.apply(testStream)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(MajorKeyWindowSnapshot.<TSAccum>generateWindowSnapshot());

    // Extract the values that we need to check the output.
    // Place the major and minor key as TSKey
    PCollection<TSKey> windowKeys = result.apply(ParDo.of(new IterableTSAccumTSKeyDoFn()));

    PAssert.that(windowKeys)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(5)))
        .containsInAnyOrder(TSDataTestUtils.KEY_A_A, TSDataTestUtils.KEY_B_A)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS), Duration.standardSeconds(5)))
        .containsInAnyOrder(
            TSDataTestUtils.KEY_A_A,
            TSDataTestUtils.KEY_A_B,
            TSDataTestUtils.KEY_B_A,
            TSDataTestUtils.KEY_B_B);

    p.run();
  }

  @Test
  public void testFromTSAccumSequenceMajorKey() {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    TestStream<KV<TSKey, TSAccumSequence>> testStream =
        TestStream.create(CommonUtils.getKvTSAccumSequenceCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(KV.of(TSDataTestUtils.KEY_A_A, FIRST_ACCUM_SEQ_A_A))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS))
            .addElements(
                KV.of(TSDataTestUtils.KEY_A_A, SECOND_ACCUM_SEQ_A_A),
                KV.of(TSDataTestUtils.KEY_A_B, SECOND_ACCUM_SEQ_A_B),
                KV.of(TSDataTestUtils.KEY_B_A, SECOND_ACCUM_SEQ_B_A))
            .advanceWatermarkToInfinity();

    PCollection<Iterable<TSAccumSequence>> result =
        p.apply(testStream)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(MajorKeyWindowSnapshot.<TSAccumSequence>generateWindowSnapshot());

    // Extract the values that we need to check the output.
    // Place the major and minor key as TSKey
    PCollection<TSKey> windowKeys = result.apply(ParDo.of(new IterableTSAccumSequenceTSKeyDoFn()));

    PAssert.that(windowKeys)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(5)))
        .containsInAnyOrder(TSDataTestUtils.KEY_A_A)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS), Duration.standardSeconds(5)))
        .containsInAnyOrder(
            TSDataTestUtils.KEY_A_A, TSDataTestUtils.KEY_A_B, TSDataTestUtils.KEY_B_A);

    p.run();
  }

  @Test
  public void testFromTSAccumMultipltMinorKeys() {

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    TestStream<KV<TSKey, TSAccum>> testStream =
        TestStream.create(CommonUtils.getKvTSAccumCoder())
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.START))
            .addElements(
                KV.of(TSDataTestUtils.KEY_A_A, FIRST_ACCUM_A_A),
                KV.of(TSDataTestUtils.KEY_A_A, FIRST_ACCUM_A_A),
                KV.of(TSDataTestUtils.KEY_A_B, FIRST_ACCUM_A_B))
            .advanceWatermarkTo(Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS))
            .addElements(
                KV.of(TSDataTestUtils.KEY_A_B, SECOND_ACCUM_A_B),
                KV.of(TSDataTestUtils.KEY_A_C, SECOND_ACCUM_A_C),
                KV.of(TSDataTestUtils.KEY_B_A, SECOND_ACCUM_B_A),
                KV.of(TSDataTestUtils.KEY_B_A, SECOND_ACCUM_B_A))
            .advanceWatermarkToInfinity();

    PCollection<Iterable<TSAccum>> result =
        p.apply(testStream)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(MinorKeyWindowSnapshot.<TSAccum>generateWindowSnapshot());

    // Extract the values that we need to check the output.
    // Place the major and minor key as TSKey
    PCollection<Long> windowKeys = result.apply(ParDo.of(new ExtractSequenceCountPerMajorKey()));

    PAssert.that(windowKeys)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.START), Duration.standardSeconds(5)))
        .containsInAnyOrder(3L)
        .inWindow(
            new IntervalWindow(
                Instant.ofEpochMilli(TSDataTestUtils.PLUS_FIVE_SECS), Duration.standardSeconds(5)))
        .containsInAnyOrder(2L, 2L);

    p.run();
  }

  public static class ExtractSequenceCountPerMajorKey extends DoFn<Iterable<TSAccum>, Long> {
    @ProcessElement
    public void process(@Element Iterable<TSAccum> accums, OutputReceiver<Long> o) {
      o.output(StreamSupport.stream(accums.spliterator(), true).count());
    }
  }
}
