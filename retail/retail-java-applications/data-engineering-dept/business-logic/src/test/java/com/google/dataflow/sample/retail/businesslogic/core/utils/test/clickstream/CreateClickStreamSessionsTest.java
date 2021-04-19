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

import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.ClickStreamSessions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
/** Unit tests for {@link CreateClickStreamSessions}. */
public class CreateClickStreamSessionsTest {

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testSessionization() throws Exception {

    Duration windowDuration = Duration.standardMinutes(5);

    PCollection<List<Long>> sessions =
        pipeline
            .apply(
                Create.timestamped(
                    ClickStreamSessionTestUtil.CLICK_STREAM_EVENT_0_MINS,
                    ClickStreamSessionTestUtil.CLICK_STREAM_EVENT_3_MINS,
                    ClickStreamSessionTestUtil.CLICK_STREAM_EVENT_10_MINS))
            .apply(Convert.toRows())
            .apply(ClickStreamSessions.create(windowDuration))
            .apply(Select.fieldNames("value.timestamp"))
            .apply(ParDo.of(new ExtractTimestampFromRow()));

    PAssert.that(sessions)
        .containsInAnyOrder(
            ImmutableList.of(
                ClickStreamSessionTestUtil.TIME,
                Instant.ofEpochMilli(ClickStreamSessionTestUtil.TIME)
                    .plus(Duration.standardMinutes(3))
                    .getMillis()),
            ImmutableList.of(
                Instant.ofEpochMilli(ClickStreamSessionTestUtil.TIME)
                    .plus(Duration.standardMinutes(10))
                    .getMillis()));

    pipeline.run();
  }

  static class ExtractTimestampFromRow extends DoFn<Row, List<Long>> {

    @ProcessElement
    public void process(ProcessContext pc) {
      List<Long> timestamps = new ArrayList<>();
      pc.element().getArray("timestamp").forEach(x -> timestamps.add((Long) x));
      Collections.sort(timestamps);
      pc.output(timestamps);
    }
  }
}
