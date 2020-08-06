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
package com.google.dataflow.sample.timeseriesflow.transforms;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.extensions.protobuf.ProtoMessageSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Row.FieldValueBuilder;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Instant;

@Experimental
/** Convert a {@link TSAccum} to a {@link Row}. */
public class TSAccumToRow extends PTransform<PCollection<TSAccum>, PCollection<Row>> {

  public static final String TIMESERIES_MAJOR_KEY = "timeseries_key";
  public static final String TIMESERIES_MINOR_KEY = "timeseries_minor_key";
  public static final String UPPER_WINDOW_BOUNDARY = "upper_window_boundary";
  public static final String LOWER_WINDOW_BOUNDARY = "lower_window_boundary";
  public static final String IS_GAP_FILL_VALUE = "is_gap_fill_value";
  public static final String DATA = "data";

  @Override
  public PCollection<Row> expand(PCollection<TSAccum> input) {

    input
        .getPipeline()
        .getSchemaRegistry()
        .registerSchemaProvider(TSAccum.class, new ProtoMessageSchema());

    return input
        .apply(MapElements.into(TypeDescriptors.rows()).via(toRow()))
        .setRowSchema(tsAccumRowSchema());
  }

  public static SerializableFunction<TSAccum, Row> toRow() {

    return new SerializableFunction<TSAccum, Row>() {
      @Override
      public Row apply(TSAccum input) {

        FieldValueBuilder row =
            Row.withSchema(tsAccumRowSchema())
                .withFieldValue(TIMESERIES_MAJOR_KEY, input.getKey().getMajorKey())
                .withFieldValue(TIMESERIES_MINOR_KEY, input.getKey().getMinorKeyString())
                .withFieldValue(IS_GAP_FILL_VALUE, input.getHasAGapFillMessage())
                .withFieldValue(
                    LOWER_WINDOW_BOUNDARY,
                    Instant.ofEpochMilli(Timestamps.toMillis(input.getLowerWindowBoundary())))
                .withFieldValue(
                    UPPER_WINDOW_BOUNDARY,
                    Instant.ofEpochMilli(Timestamps.toMillis(input.getUpperWindowBoundary())));

        List<Row> rows = new ArrayList<>();

        for (Entry<String, Data> entry : input.getDataStoreMap().entrySet()) {
          rows.add(TSDataUtils.generateValueRow(entry.getKey(), entry.getValue()));
        }

        return row.withFieldValue(DATA, rows).build();
      }
    };
  }

  public static Schema tsAccumRowSchema() {

    return Schema.builder()
        .addStringField(TIMESERIES_MAJOR_KEY)
        .addStringField(TIMESERIES_MINOR_KEY)
        .addDateTimeField(LOWER_WINDOW_BOUNDARY)
        .addDateTimeField(UPPER_WINDOW_BOUNDARY)
        .addBooleanField(IS_GAP_FILL_VALUE)
        .addArrayField(DATA, FieldType.row(TSDataUtils.getDataRowSchema()))
        .build();
  }
}
