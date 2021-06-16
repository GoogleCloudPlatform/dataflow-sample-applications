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
package com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/** Objects to map to Error Message events */
@Experimental
class GenerateErrorMessageEvents {

  /** Create ErrorMessage for JsonToRow Conversation failed records */
  public static class GenerateJsonToRowErrorMessage extends DoFn<Row, Row> {

    Schema errorMessageSchema;

    public GenerateJsonToRowErrorMessage(Schema errorMessageSchema) {
      this.errorMessageSchema = errorMessageSchema;
    }

    @ProcessElement
    public void processElement(
        @Element Row element,
        @FieldAccess("line") String inputData,
        @FieldAccess("err") String errorMessage,
        @Timestamp Instant timestamp,
        OutputReceiver<Row> out) {

      out.output(
          Row.withSchema(errorMessageSchema)
              .withFieldValue("inputData", inputData)
              .withFieldValue("errorMessage", errorMessage)
              .withFieldValue("timestamp", timestamp)
              .build());
    }
  }

  /** Create Generalized ErrorMessage for custom error messages */
  public static class GenerateGenericErrorMessage<T> extends DoFn<T, Row> {

    Schema errorMessageSchema;
    String errorMessage;

    private final Counter numberOfFailedParsedEvents =
        Metrics.counter(GenerateGenericErrorMessage.class, "numberOfFailedParsedEvents");

    public GenerateGenericErrorMessage(Schema errorMessageSchema, String errorMessage) {
      this.errorMessageSchema = errorMessageSchema;
      this.errorMessage = errorMessage;
    }

    @ProcessElement
    public void processElement(
        @Element T element, @Timestamp Instant timestamp, OutputReceiver<Row> out) {

      numberOfFailedParsedEvents.inc();
      out.output(
          Row.withSchema(errorMessageSchema)
              .withFieldValue("inputData", element.toString())
              .withFieldValue("errorMessage", errorMessage)
              .withFieldValue("timestamp", timestamp)
              .build());
    }
  }
}
