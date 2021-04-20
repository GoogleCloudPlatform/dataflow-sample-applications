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

import com.google.dataflow.sample.retail.businesslogic.core.utils.JSONUtils;
import com.google.dataflow.sample.retail.businesslogic.core.utils.test.avrotestobjects.ClickStreamEventAVRO;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.GsonBuilder;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link JsonToRow}. */
@RunWith(JUnit4.class)
public class JSONUtilsTest {
  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static final ClickStreamEvent AUTO_VALUE_EVENT =
      ClickStreamEvent.builder()
          .setUid(1L)
          .setClientId("1")
          .setAgent("A")
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();

  private ClickStreamEventAVRO getEventAVRO() {
    ClickStreamEventAVRO event = new ClickStreamEventAVRO();
    event.clientId = "1";
    event.uid = 1L;
    event.agent = "A";
    event.pageRef = "pageRef";
    event.pageTarget = "pageTarget";
    event.event = "event";
    event.timestamp = TIME;

    return event;
  }

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParseCleanClickstream() {

    Gson gson = new Gson();
    String jsonString = gson.toJson(getEventAVRO());

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(jsonString))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).containsInAnyOrder(AUTO_VALUE_EVENT);

    pipeline.run();
  }

  @Test
  public void testParseWithStrictNullsClickstream() {

    ClickStreamEventAVRO withNull = getEventAVRO();
    withNull.uid = null;

    Gson gson = new GsonBuilder().serializeNulls().create();

    String jsonString = gson.toJson(withNull);

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(jsonString))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).containsInAnyOrder(AUTO_VALUE_EVENT.toBuilder().setUid(null).build());

    pipeline.run();
  }

  @Test
  public void testParseWithNonStrictNullsClickstream() {

    ClickStreamEventAVRO withNull = getEventAVRO();
    withNull.uid = null;

    Gson gson = new GsonBuilder().create();

    String jsonString = gson.toJson(withNull);

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(jsonString))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).containsInAnyOrder(AUTO_VALUE_EVENT.toBuilder().setUid(null).build());

    pipeline.run();
  }

  @Test
  public void testParseWithTypeErrorClickstream() {

    Gson gson = new Gson();
    String jsonString = gson.toJson(getEventAVRO());
    jsonString = jsonString.replace("\"A\"", "1.0");

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(jsonString))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).empty();

    pipeline.run();
  }
}
