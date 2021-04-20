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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream;

import com.google.auto.value.AutoValue;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;

@AutoValue
/**
 * Takes a ROW generated with the {@link ClickStreamSessions} transform and back propagates fields
 * throughout the session. Given a list of {@link
 * com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent} the fields provided
 * in {@link BackFillSessionData#getBackPropogateFields()} will be back propagated to all events
 * ordered by event time.
 *
 * <p>For example if foo is the field that is passed to {@link
 * BackFillSessionData#getBackPropogateFields()} then the first occurrence of foo will be propagated
 * to any events that have foo == null *
 *
 * <pre>{@code
 * Given the following input
 * { key : sess_1
 *  [
 *  {event_1 : { timestamp : 1,  foo : null, bar : cup}}
 *  {event_2 : { timestamp : 2,  foo : null, bar : mug}}
 *  {event_3 : { timestamp : 3,  foo : 765   , bar : bottle }}
 *  {event_4 : { timestamp : 3,  foo : null   , bar : bottle }}
 *  ]
 * }
 *
 * The output will become
 * { key : sess_1
 *  [
 *  {event_1 : { timestamp : 1,  foo : 765, bar : cup}}
 *  {event_2 : { timestamp : 2,  foo : 765, bar : mug}}
 *  {event_3 : { timestamp : 3,  foo : 765, bar : bottle }}
 *  {event_4 : { timestamp : 4,  foo : null   , bar : bottle }}
 *  ]
 * }
 * }</pre>
 *
 * If timestamp is missing from any of the elements, no correction is made as values can not be
 * sorted by time order. Only values before the first non-null value are filled, values after the
 * first non-null are assumed to be correct.
 */
public abstract class BackFillSessionData extends PTransform<PCollection<Row>, PCollection<Row>> {

  public abstract List<String> getBackPropogateFields();

  public static BackFillSessionData create(List<String> newBackPropogateFields) {
    return builder().setBackPropogateFields(newBackPropogateFields).build();
  }

  public static Builder builder() {
    return new AutoValue_BackFillSessionData.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setBackPropogateFields(List<String> backPropogateFields);

    public abstract BackFillSessionData build();
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    return input
        .apply(
            MapElements.into(TypeDescriptors.rows())
                .via(x -> backPropogate(x, getBackPropogateFields())))
        .setRowSchema(input.getSchema());
  }

  public static Row backPropogate(Row input, List<String> fieldNames) {

    Iterable<Row> rows = input.getIterable("value");

    List<Row> sortedRows =
        StreamSupport.stream(rows.spliterator(), false).collect(Collectors.toList());

    try {
      sortedRows.sort(
          new Comparator<Row>() {

            @Override
            public int compare(Row o1, Row o2) {
              Long timestamp1 = o1.getValue("timestamp");
              Long timestamp2 = o2.getValue("timestamp");
              Preconditions.checkNotNull(timestamp1);
              Preconditions.checkNotNull(timestamp2);
              return timestamp1.compareTo(timestamp2);
            }
          });
    } catch (NullPointerException npe) {
      // If any of the timestamps are null we output without back propagation.
      return input;
    }

    // Loop through for all requested fieldNames
    for (String fieldName : fieldNames) {

      // Find the first value which has a non null value
      List<Row> correctedRows = new ArrayList<>();

      int pos =
          IntStream.range(0, sortedRows.size())
              .filter(i -> sortedRows.get(i).getValue(fieldName) != null)
              .findFirst() // first occurrence
              .orElse(-1); // No element found

      System.out.println(sortedRows);

      // Only correct values if there is at least one non-null values for the field and the first
      // non-null value is not the first value seen
      if (pos > 0) {
        Row value = sortedRows.get(pos);
        Object changeField = value.getValue(fieldName);

        // Update any fields that are empty in the list, stop when getting to first known non null
        // pos
        for (int i = 0; i < pos; i++) {
          if (sortedRows.get(i).getValue(fieldName) == null) {
            correctedRows.add(Row.fromRow(value).withFieldValue(fieldName, changeField).build());
          } else {
            correctedRows.add(sortedRows.get(i));
          }
        }
        // Add the rest of the fields from pos onwards
        IntStream.range(pos, sortedRows.size()).forEach(x -> correctedRows.add(sortedRows.get(x)));
      }
      if (correctedRows.size() > 0) {
        sortedRows.clear();
        sortedRows.addAll(correctedRows);
        correctedRows.clear();
      }
    }
    return Row.fromRow(input).withFieldValue("value", sortedRows).build();
  }
}
