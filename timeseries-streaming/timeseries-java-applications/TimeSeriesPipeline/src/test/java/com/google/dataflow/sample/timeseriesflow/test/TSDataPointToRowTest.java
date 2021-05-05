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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.TSDataPointToRow;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TSDataPointToRowTest {

  @Test
  @Ignore
  // TODO correct bug with bytebuddy and the TSDatapoint extendedData Map.
  public void testToRow() {

    Instant instant = Instant.parse("2000-01-01T00:00:00");
    Timestamp timeA = Timestamps.fromMillis(instant.getMillis());

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    TSKey key = TSKey.newBuilder().setMajorKey("Major").setMajorKey("Minor").build();
    TSDataPoint a =
        TSDataPoint.newBuilder()
            .setKey(key)
            .setTimestamp(timeA)
            .setData(Data.newBuilder().setIntVal(1))
            .putExtendedData("a", CommonUtils.createNumData(0))
            .build();

    p.getSchemaRegistry().registerSchemaProvider(TSDataPoint.class, new ProtoMessageSchema());

    PCollection<TSDataPoint> row =
        p.apply(Create.of(a))
            .apply(new TSDataPointToRow())
            .apply(Convert.fromRows(TSDataPoint.class));

    PAssert.that(row).containsInAnyOrder(a);

    p.run();
  }
}
