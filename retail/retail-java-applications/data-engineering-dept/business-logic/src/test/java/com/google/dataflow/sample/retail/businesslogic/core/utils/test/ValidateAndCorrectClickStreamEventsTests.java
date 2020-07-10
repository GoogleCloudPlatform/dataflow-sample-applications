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
package com.google.dataflow.sample.retail.businesslogic.core.utils.test;

import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.ValidateAndCorrectClickStreamEvents;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JsonToRow}. */
@RunWith(JUnit4.class)
public class ValidateAndCorrectClickStreamEventsTests {
  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static final ClickStreamEvent CLEAN_DATA =
      ClickStreamEvent.builder()
          .setUid(1L)
          .setSessionId("1")
          .setAgent("A")
          .setLng(0.3678D)
          .setLat(51.5466D)
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  private static final ClickStreamEvent MISSING_LAT_LNG =
      ClickStreamEvent.builder()
          .setUid(1L)
          .setSessionId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  private static final ClickStreamEvent MISSING_UID =
      ClickStreamEvent.builder()
          .setSessionId("1")
          .setAgent("A")
          .setLng(0.3678D)
          .setLat(51.5466D)
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  private static final ClickStreamEvent MISSING_UID_LAT_LNG =
      ClickStreamEvent.builder()
          .setSessionId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCleanValidation() {

    PCollection<ClickStreamEvent> result =
        pipeline.apply(Create.of(CLEAN_DATA)).apply(new ValidateAndCorrectClickStreamEvents());

    PAssert.that(result).containsInAnyOrder(CLEAN_DATA);
    pipeline.run();
  }

  @Test
  public void testLatLng() {

    PCollection<ClickStreamEvent> result =
        pipeline.apply(Create.of(MISSING_LAT_LNG)).apply(new ValidateAndCorrectClickStreamEvents());

    PAssert.that(result).containsInAnyOrder(CLEAN_DATA);
    pipeline.run();
  }

  @Test
  public void testMissingUID() {

    PCollection<ClickStreamEvent> result =
        pipeline.apply(Create.of(MISSING_UID)).apply(new ValidateAndCorrectClickStreamEvents());

    // Random number is attached to UID in this sample app. So can not test for the whole object.

    PCollection<ClickStreamEvent> output =
        result.apply(
            Filter.by(
                (SerializableFunction<ClickStreamEvent, Boolean>) input -> input.getUid() == null));

    PAssert.that(output).empty();
    pipeline.run();
  }

  @Test
  public void testMissingUIDAndLatLng() {

    PCollection<ClickStreamEvent> result =
        pipeline
            .apply(Create.of(MISSING_UID_LAT_LNG))
            .apply(new ValidateAndCorrectClickStreamEvents());

    // Random number is attached to UID in this sample app. So can not test for the whole object.

    PCollection<ClickStreamEvent> output =
        result.apply(
            Filter.by(
                (SerializableFunction<ClickStreamEvent, Boolean>)
                    input ->
                        input.getUid() == null
                            || input.getLat() == null
                            || input.getLng() == null));

    PAssert.that(output).empty();
    pipeline.run();
  }
}
