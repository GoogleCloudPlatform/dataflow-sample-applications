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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.PerfectRectangles;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PopulateDefaultValuesTests {

  // TODO add missing middle gap tests
  @Test
  public void testWithinTTLSingleKeyFillWithZeroValue() {

    Instant now = Instant.parse("2000-01-01T00:00:00Z");
    Duration ttlDuration = Duration.standardSeconds(2);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    TSDataPoint dataPoint =
        TSDataPoint.newBuilder()
            .setKey(TSKey.newBuilder().setMajorKey("A").setMinorKeyString("B").build())
            .setTimestamp(Timestamps.fromMillis(now.getMillis()))
            .build();

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(Create.timestamped(TimestampedValue.of(KV.of(dataPoint.getKey(), dataPoint), now)))
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(
                    Duration.standardSeconds(1), ttlDuration))
            .apply(Values.create());

    PAssert.that(perfectRectangle)
        .containsInAnyOrder(
            dataPoint,
            dataPoint
                .toBuilder()
                .setData(TSDataUtils.getZeroValueForType(dataPoint.getData()))
                .setIsAGapFillMessage(true)
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(2)).getMillis() - 1))
                .build(),
            dataPoint
                .toBuilder()
                .setData(TSDataUtils.getZeroValueForType(dataPoint.getData()))
                .setIsAGapFillMessage(true)
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(3)).getMillis() - 1))
                .build());

    p.run();
  }

  @Test
  public void testWithinTTLSingleKeySingleFillWithLastKnownValue() {

    Instant now = Instant.parse("2000-01-01T00:00:00Z");
    Duration ttlDuration = Duration.standardSeconds(1);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    TSDataPoint dataPoint =
        TSDataPoint.newBuilder()
            .setKey(TSKey.newBuilder().setMajorKey("A").setMinorKeyString("B").build())
            .setTimestamp(Timestamps.fromMillis(now.getMillis()))
            .build();

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(Create.timestamped(TimestampedValue.of(KV.of(dataPoint.getKey(), dataPoint), now)))
            .apply(
                PerfectRectangles.withWindowAndTTLDuration(Duration.standardSeconds(1), ttlDuration)
                    .enablePreviousValueFill())
            .apply(Values.create());

    PAssert.that(perfectRectangle)
        .containsInAnyOrder(
            dataPoint,
            dataPoint
                .toBuilder()
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(2)).getMillis() - 1))
                .setIsAGapFillMessage(true)
                .build());

    p.run();
  }

  @Test
  public void testWithinAbsoluteBound() {

    Instant now = Instant.parse("2000-01-01T00:00:00Z");

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

    TSDataPoint dataPoint =
        TSDataPoint.newBuilder()
            .setKey(TSKey.newBuilder().setMajorKey("A").setMinorKeyString("B").build())
            .setTimestamp(Timestamps.fromMillis(now.getMillis()))
            .build();

    PCollection<TSDataPoint> perfectRectangle =
        p.apply(Create.timestamped(TimestampedValue.of(KV.of(dataPoint.getKey(), dataPoint), now)))
            .apply(
                PerfectRectangles.withWindowAndAbsoluteStop(
                        Duration.standardSeconds(1), Instant.parse("2000-01-01T00:00:02Z"))
                    .enablePreviousValueFill())
            .apply(Values.create());

    PAssert.that(perfectRectangle)
        .containsInAnyOrder(
            dataPoint,
            dataPoint
                .toBuilder()
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(2)).getMillis() - 1))
                .setIsAGapFillMessage(true)
                .build(),
            dataPoint
                .toBuilder()
                .setTimestamp(
                    Timestamps.fromMillis(now.plus(Duration.standardSeconds(3)).getMillis() - 1))
                .setIsAGapFillMessage(true)
                .build());

    p.run();
  }
}
