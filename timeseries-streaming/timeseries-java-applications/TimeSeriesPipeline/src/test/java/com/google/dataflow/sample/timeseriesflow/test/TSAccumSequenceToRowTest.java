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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.transforms.TSAccumSequenceToRow;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TSAccumSequenceToRowTest {

  @Test
  public void testToRow() {

    Instant instant = Instant.parse("2000-01-01T00:00:00");

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    TSKey key = TSKey.newBuilder().setMajorKey("Major").setMajorKey("Minor").build();
    Data data = Data.newBuilder().setCategoricalVal("a").build();
    TSAccumSequence a =
        TSAccumSequence.newBuilder()
            .setKey(key)
            .setLowerWindowBoundary(Timestamps.fromMillis(instant.getMillis()))
            .setUpperWindowBoundary(Timestamps.fromMillis(instant.getMillis()))
            .putSequenceData("a", data)
            .build();

    p.getSchemaRegistry().registerSchemaProvider(TSAccumSequence.class, new ProtoMessageSchema());

    PCollection<TSAccumSequence> row =
        p.apply(Create.of(a))
            .apply(new TSAccumSequenceToRow())
            .apply(Convert.fromRows(TSAccumSequence.class));

    PAssert.that(row).containsInAnyOrder(a);

    p.run();
  }
}
