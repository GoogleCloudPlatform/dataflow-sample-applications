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
package com.google.dataflow.sample.retail.businesslogic.core.utils;

import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import com.google.gson.Gson;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;

@Experimental
public class Mocks {

  public static PCollection<String> getClickStreamPubSubMock(Pipeline p) {
    return p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1)))
        .apply(MapElements.into(TypeDescriptors.strings()).via(Mocks::getMock));
  }

  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static String getMock(Long futureSec) {
    Gson gson = new Gson();

    return gson.toJson(
        AUTO_VALUE_EVENT
            .toBuilder()
            .setTimestamp(Instant.ofEpochMilli(TIME).plus(futureSec).getMillis())
            .build());
  }

  private static final ClickStreamEvent AUTO_VALUE_EVENT =
      ClickStreamEvent.builder()
          .setUid(1L)
          .setSessionId("1")
          .setAgent("A")
          .setLng(1D)
          .setLat(1D)
          .setPageRef("pageRef")
          .setPageTarget("pageTarget")
          .setEvent("event")
          .setUid(1L)
          .setTimestamp(TIME)
          .build();
}
