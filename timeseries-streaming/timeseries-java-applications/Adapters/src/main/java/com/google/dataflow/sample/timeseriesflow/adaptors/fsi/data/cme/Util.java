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

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TopOfBook.TopOfBookEvent;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TradeInfo.TradeInfoEvent;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
class Util {

  public static final Logger LOG = LoggerFactory.getLogger(Util.class);

  public static final TupleTag<Row> extractAskBidSuccess = new TupleTag<Row>() {};

  public static final TupleTag<TopOfBookEvent> extractAskBidFailure =
      new TupleTag<TopOfBookEvent>() {};

  public static final TupleTag<Row> extractTradeInfoSuccess = new TupleTag<Row>() {};

  public static final TupleTag<TradeInfoEvent> extractTradeInfoFailure =
      new TupleTag<TradeInfoEvent>() {};

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  /* Represents Error Message event */
  public abstract static class ErrorMessage {

    @SchemaFieldName("inputData")
    public abstract String getInputData();

    @SchemaFieldName("errorMessage")
    public abstract String getErrorMessage();

    @SchemaFieldName("timestamp")
    public abstract Instant getTimestamp();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_Util_ErrorMessage.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setInputData(String value);

      public abstract Builder setErrorMessage(String value);

      public abstract Builder setTimestamp(Instant value);

      public abstract ErrorMessage build();
    }
  }
}
