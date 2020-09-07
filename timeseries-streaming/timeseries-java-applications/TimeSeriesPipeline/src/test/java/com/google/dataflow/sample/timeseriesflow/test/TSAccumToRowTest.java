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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.transforms.TSAccumToRow;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TSAccumToRowTest {

  @Test
  public void testToRow() {

    Instant instant = Instant.parse("2000-01-01T00:00:00");
    Timestamp timeA = Timestamps.fromMillis(instant.getMillis());

    Pipeline p = Pipeline.create(PipelineOptionsFactory.create());
    TSKey key = TSKey.newBuilder().setMajorKey("Major").setMinorKeyString("Minor").build();

    TSAccum a =
        TSAccum.newBuilder()
            .setKey(key)
            .setLowerWindowBoundary(timeA)
            .setUpperWindowBoundary(timeA)
            .setHasAGapFillMessage(true)
            .putDataStore("int", Data.newBuilder().setIntVal(1).build())
            .putDataStore("dbl", Data.newBuilder().setDoubleVal(1).build())
            .putDataStore("lng", Data.newBuilder().setLongVal(1).build())
            .putDataStore("flt", Data.newBuilder().setFloatVal(1).build())
            .putDataStore("str", Data.newBuilder().setCategoricalVal("1").build())
            .build();

    PCollection<Row> row = p.apply(Create.of(a)).apply(new TSAccumToRow());

    List<Row> rows = new ArrayList<>();

    rows.add(
        Row.withSchema(TSDataUtils.getDataRowSchema())
            .withFieldValue(TSDataUtils.METRIC_NAME, "int")
            .withFieldValue(TSDataUtils.INTEGER_ROW_NAME, 1)
            .build());
    rows.add(
        Row.withSchema(TSDataUtils.getDataRowSchema())
            .withFieldValue(TSDataUtils.METRIC_NAME, "dbl")
            .withFieldValue(TSDataUtils.DOUBLE_ROW_NAME, 1D)
            .build());
    rows.add(
        Row.withSchema(TSDataUtils.getDataRowSchema())
            .withFieldValue(TSDataUtils.METRIC_NAME, "lng")
            .withFieldValue(TSDataUtils.LONG_ROW_NAME, 1L)
            .build());
    rows.add(
        Row.withSchema(TSDataUtils.getDataRowSchema())
            .withFieldValue(TSDataUtils.METRIC_NAME, "flt")
            .withFieldValue(TSDataUtils.FLOAT_ROW_NAME, 1F)
            .build());
    rows.add(
        Row.withSchema(TSDataUtils.getDataRowSchema())
            .withFieldValue(TSDataUtils.METRIC_NAME, "str")
            .withFieldValue(TSDataUtils.STRING_ROW_NAME, "1")
            .build());

    Row testRow =
        Row.withSchema(TSAccumToRow.tsAccumRowSchema())
            .withFieldValue(
                TSAccumToRow.LOWER_WINDOW_BOUNDARY,
                Instant.ofEpochMilli(Timestamps.toMillis(timeA)))
            .withFieldValue(
                TSAccumToRow.UPPER_WINDOW_BOUNDARY,
                Instant.ofEpochMilli(Timestamps.toMillis(timeA)))
            .withFieldValue(TSAccumToRow.TIMESERIES_MAJOR_KEY, "Major")
            .withFieldValue(TSAccumToRow.TIMESERIES_MINOR_KEY, "Minor")
            .withFieldValue(TSAccumToRow.IS_GAP_FILL_VALUE, true)
            .withFieldValue(TSAccumToRow.DATA, rows)
            .build();

    PAssert.that(row).containsInAnyOrder(testRow);

    p.run();
  }
}
