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
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;

/*
{
    "header": {
        "messageType": "OBTS",
        "sentTime": "2020-08-04T17:07:51.382813700",
        "version": "1.0"
    },
    "payload": [{
        "tradingStatus": "ReadyToTrade",
        "instrument": {
            "definitionSource": "E",
            "exchangeMic": "XCME",
            "id": "16337",
            "marketSegmentId": "68",
            "periodCode": "202012",
            "productCode": "NQ",
            "productGroup": "NQ",
            "productType": "FUT",
            "symbol": "NQZ0"
        },
        "askLevel": [{
            "lastUpdateTime": "2020-08-04T17:07:51.382681973",
            "orderCnt": 1,
            "price": "1105500",
            "qty": 1
        }],
        "bidLevel": [{
            "lastUpdateTime": "2020-08-04T17:07:51.382681973",
            "orderCnt": 1,
            "price": "1105175",
            "qty": 1
        }]
    }]
}
 */

/** Objects to map to Top of Book */
@Experimental
class TopOfBook {

  /**
   * Represents <a
   * href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON#CMESmartStreamonGCPJSON-BookMessage">Top
   * Of Book</a> event
   */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class TopOfBookEvent {

    @SchemaFieldName("header")
    public abstract Header getHeader();

    @SchemaFieldName("payload")
    public abstract List<Payload> getPayload();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TopOfBook_TopOfBookEvent.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setHeader(Header value);

      public abstract Builder setPayload(List<Payload> value);

      public abstract TopOfBookEvent build();
    }
  }

  /** Represents Header event inside of Top Of Book event */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Header {

    @SchemaFieldName("messageType")
    public abstract String getMessageType();

    @SchemaFieldName("sentTime")
    public abstract String getSentTime();

    @SchemaFieldName("version")
    public abstract String getVersion();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TopOfBook_Header.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setMessageType(String value);

      public abstract Builder setSentTime(String value);

      public abstract Builder setVersion(String value);

      public abstract Header build();
    }
  }

  /** Represents Payload event inside of Top Of Book event */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Payload {

    @SchemaFieldName("tradingStatus")
    public abstract String getTradingStatus();

    @SchemaFieldName("instrument")
    public abstract Instrument getInstrument();

    @SchemaFieldName("askLevel")
    public abstract List<PriceLevel> getAskLevel();

    @SchemaFieldName("bidLevel")
    public abstract List<PriceLevel> getBidLevel();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TopOfBook_Payload.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setTradingStatus(String value);

      public abstract Builder setInstrument(Instrument value);

      public abstract Builder setAskLevel(List<PriceLevel> value);

      public abstract Builder setBidLevel(List<PriceLevel> value);

      public abstract Payload build();
    }
  }

  /** Represents PriceLevel event inside of Payload within Top of Book event */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class PriceLevel {

    @SchemaFieldName("price")
    public @Nullable abstract String getPrice();

    @SchemaFieldName("lastUpdateTime")
    public abstract String getLastUpdateTime();

    @SchemaFieldName("orderCnt")
    public @Nullable abstract Long getOrderCnt();

    @SchemaFieldName("qty")
    public @Nullable abstract Long getQty();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TopOfBook_PriceLevel.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setPrice(String value);

      public abstract Builder setLastUpdateTime(String value);

      public abstract Builder setOrderCnt(Long value);

      public abstract Builder setQty(Long value);

      public abstract PriceLevel build();
    }
  }

  /** Represents Instrument event inside of Payload within Top of Book event */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Instrument {

    @SchemaFieldName("definitionSource")
    public abstract String getDefinitionSource();

    @SchemaFieldName("exchangeMic")
    public abstract String getExchangeMic();

    @SchemaFieldName("id")
    public abstract String getId();

    @SchemaFieldName("marketSegmentId")
    public abstract String getMarketSegmentId();

    @SchemaFieldName("periodCode")
    public abstract String getPeriodCode();

    @SchemaFieldName("productCode")
    public abstract String getProductCode();

    @SchemaFieldName("productGroup")
    public abstract String getProductGroup();

    @SchemaFieldName("productType")
    public abstract String getProductType();

    @SchemaFieldName("symbol")
    public abstract String getSymbol();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TopOfBook_Instrument.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setDefinitionSource(String value);

      public abstract Builder setExchangeMic(String value);

      public abstract Builder setId(String value);

      public abstract Builder setMarketSegmentId(String value);

      public abstract Builder setPeriodCode(String value);

      public abstract Builder setProductCode(String value);

      public abstract Builder setProductGroup(String value);

      public abstract Builder setProductType(String value);

      public abstract Builder setSymbol(String value);

      public abstract Instrument build();
    }
  }
}
