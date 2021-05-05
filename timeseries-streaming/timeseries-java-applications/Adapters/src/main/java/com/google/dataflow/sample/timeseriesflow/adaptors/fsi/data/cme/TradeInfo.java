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
        "messageType": "TS",
        "sentTime": "null",
        "version": "1.0"
    },
    "payload": [{
        "lastUpdateTime": "2020-10-19T20:18:44.872357987",
        "tradeSummary": {
            "aggressorSide": "1",
            "mdTradeEntryId": "11323435",
            "tradePrice": "4094",
            "tradeQty": "1",
            "tradeOrderCount": "2",
            "tradeUpdateAction": "NEW",
            "orderQty": {
                "orderId": "2",
                "lastOrdQty": "1"
            }
        },
        "instrument": {
            "definitionSource": "E",
            "exchangeMic": "XNYM",
            "id": "207193",
            "marketSegmentId": "80",
            "periodCode": "202012",
            "productCode": "CL",
            "productType": "FUT",
            "productGroup": "CL",
            "symbol": "CLZ0"
        }
    }]
}
 */

/** Objects to map to Trade Info */
@Experimental
class TradeInfo {

  /** Represents Trade Info event */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class TradeInfoEvent {

    @SchemaFieldName("header")
    public abstract Header getHeader();

    @SchemaFieldName("payload")
    public abstract List<Payload> getPayload();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TradeInfo_TradeInfoEvent.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setHeader(Header value);

      public abstract Builder setPayload(List<Payload> value);

      public abstract TradeInfoEvent build();
    }
  }

  /** Represents Header event inside of Trade Info event */
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
      return new AutoValue_TradeInfo_Header.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setMessageType(String value);

      public abstract Builder setSentTime(String value);

      public abstract Builder setVersion(String value);

      public abstract Header build();
    }
  }

  /** Represents Payload event inside of Trade Info event */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class Payload {

    @SchemaFieldName("lastUpdateTime")
    public abstract String getLastUpdateTime();

    @SchemaFieldName("tradeSummary")
    public abstract TradeSummary getTradeSummary();

    @SchemaFieldName("instrument")
    public abstract Instrument getInstrument();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TradeInfo_Payload.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setLastUpdateTime(String value);

      public abstract Builder setTradeSummary(TradeSummary value);

      public abstract Builder setInstrument(Instrument value);

      public abstract Payload build();
    }
  }

  /** Represents Trade Summary event inside of Payload within Trade Info event */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class TradeSummary {

    @SchemaFieldName("aggressorSide")
    public abstract String getAggressorSide();

    @SchemaFieldName("mdTradeEntryId")
    public abstract String getMdTradeEntryId();

    @SchemaFieldName("tradePrice")
    public @Nullable abstract String getTradePrice();

    @SchemaFieldName("tradeQty")
    public @Nullable abstract String getTradeQty();

    @SchemaFieldName("tradeOrderCount")
    public @Nullable abstract String getTradeOrderCount();

    @SchemaFieldName("tradeUpdateAction")
    public @Nullable abstract String getTradeUpdateAction();

    @SchemaFieldName("orderQty")
    public abstract OrderQty getOrderQty();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TradeInfo_TradeSummary.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setAggressorSide(String value);

      public abstract Builder setMdTradeEntryId(String value);

      public abstract Builder setTradePrice(String value);

      public abstract Builder setTradeQty(String value);

      public abstract Builder setTradeOrderCount(String value);

      public abstract Builder setTradeUpdateAction(String value);

      public abstract Builder setOrderQty(OrderQty value);

      public abstract TradeSummary build();
    }
  }

  /**
   * Represents Order Qty event inside of Trade Summary event inside of Payload within Trade Info
   * event
   */
  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class OrderQty {

    @SchemaFieldName("orderId")
    public @Nullable abstract String getOrderId();

    @SchemaFieldName("lastOrdQty")
    public @Nullable abstract String getLastOrdQty();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TradeInfo_OrderQty.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setOrderId(String value);

      public abstract Builder setLastOrdQty(String value);

      public abstract OrderQty build();
    }
  }

  /** Represents Instrument event inside of Payload within Trade Info event */
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

    @SchemaFieldName("productType")
    public abstract String getProductType();

    @SchemaFieldName("productGroup")
    public abstract String getProductGroup();

    @SchemaFieldName("symbol")
    public abstract String getSymbol();

    public abstract Builder toBuilder();

    public static Builder builder() {
      return new AutoValue_TradeInfo_Instrument.Builder();
    }

    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setDefinitionSource(String value);

      public abstract Builder setExchangeMic(String value);

      public abstract Builder setId(String value);

      public abstract Builder setMarketSegmentId(String value);

      public abstract Builder setPeriodCode(String value);

      public abstract Builder setProductCode(String value);

      public abstract Builder setProductType(String value);

      public abstract Builder setProductGroup(String value);

      public abstract Builder setSymbol(String value);

      public abstract Instrument build();
    }
  }
}
