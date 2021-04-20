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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.validation;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Row.FieldValueBuilder;
import org.joda.time.Instant;

public class EventDateTimeCorrectionService extends DoFn<Row, Row> {

  @ProcessElement
  public void process(@Element Row input, @Timestamp Instant time, OutputReceiver<Row> o) {

    // Pass through if items are not needed by this event.

    if (!input.getArray("errors").contains(ValidateEventDateTime.CORRECTION_TIMESTAMP)) {
      o.output(input);
      return;
    }

    FieldValueBuilder row = Row.fromRow(input.getRow("data"));

    // There are two stations where we can be in this code, the event_datetime did not parse or
    // the time was in the future.
    // In both cases the fix is to set timestamp to be the processing time.
    row.withFieldValue("timestamp", time.getMillis());
    o.output(Row.fromRow(input).withFieldValue("data", row.build()).build());
  }
}
