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
package com.google.dataflow.sample.retail.businesslogic.core.utils.test.clickstream;

import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.CountViewsPerProduct;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.PageViewAggregator;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
/** Unit tests for {@link ClickstreamProcessing}. */
public class CountViewsPerProductTest {

  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static final ClickStreamEvent EVENT =
      ClickStreamEvent.builder()
          .setUid(1L)
          .setSessionId("1")
          .setAgent("A")
          .setLng(1D)
          .setLat(1D)
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("browse")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  private static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_0_MINS =
      TimestampedValue.of(EVENT, Instant.ofEpochMilli(TIME));

  private static final TimestampedValue<ClickStreamEvent> CLICK_STREAM_EVENT_1_MINS =
      TimestampedValue.of(
          EVENT
              .toBuilder()
              .setTimestamp(
                  Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(1)).getMillis())
              .build(),
          Instant.ofEpochMilli(TIME).plus(Duration.standardMinutes(1)));

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCountViews() {

    Duration windowDuration = Duration.standardMinutes(5);

    PCollection<PageViewAggregator> countPageViews =
        pipeline
            .apply(Create.timestamped(CLICK_STREAM_EVENT_0_MINS, CLICK_STREAM_EVENT_1_MINS))
            .apply(new CountViewsPerProduct(windowDuration));

    PageViewAggregator pageViewAggregator =
        PageViewAggregator.builder()
            .setCount(2L)
            .setPageRef("pageRef")
            .setDurationMS(windowDuration.getMillis())
            .setStartTime(TIME)
            .build();

    PAssert.that(countPageViews).containsInAnyOrder(pageViewAggregator);

    pipeline.run();
  }
}
