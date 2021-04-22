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
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
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
          .setUid(999L)
          .setClientId("1")
          .setPage("pageRef")
          .setPagePrevious("pageTarget")
          .setEvent("event")
          .setTimestamp(TIME)
          .build();

  private static final String JSON =
      "{\"eventTime\":null,\"event\":\"event\",\"timestamp\":946656000000,\"user_id\":999,\"client_id\":\"1\",\"page\":\"pageRef\",\"page_previous\":\"pageTarget\",\"ecommerce\":null}\n";

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testParseCleanClickstream() {

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(JSON))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).containsInAnyOrder(AUTO_VALUE_EVENT);

    pipeline.run();
  }

  @Test
  public void testParseWithStrictNullsClickstream() {

    String jsonString = JSON.replace("999", "null");

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(jsonString))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).containsInAnyOrder(AUTO_VALUE_EVENT.toBuilder().setUid(null).build());

    pipeline.run();
  }

  @Test
  public void testParseWithNonStrictNullsClickstream() {

    String jsonString = JSON.replace("999", "null");

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(jsonString))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).containsInAnyOrder(AUTO_VALUE_EVENT.toBuilder().setUid(null).build());

    pipeline.run();
  }

  @Test
  public void testParseWithTypeErrorClickstream() {

    String jsonString = JSON.replace("\"event\"", "1.0");

    PCollection<ClickStreamEvent> events =
        pipeline
            .apply(Create.of(jsonString))
            .apply(JSONUtils.ConvertJSONtoPOJO.<ClickStreamEvent>create(ClickStreamEvent.class));

    PAssert.that(events).empty();

    pipeline.run();
  }
}
