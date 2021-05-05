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

import static org.apache.beam.sdk.schemas.Schema.toSchema;

import java.util.stream.Stream;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
class Data {

  public static final Logger LOG = LoggerFactory.getLogger(Data.class);

  /**
   * Schema to extract Ask-Bid following attributes to convert to TSDataPoint <br>
   * <br>
   * sentTime, sentTime, tradingStatus, periodCode, productCode, productType, productGroup, symbol,
   * askLevelLastUpdateTime, askLevelOrderCnt, askLevelPrice, askLevelQty, bidLevelLastUpdateTime,
   * bidLevelOrderCnt, bidLevelPrice, bidLevelQty
   */
  public static final Schema symbolAskBid =
      Stream.of(
              Schema.Field.of("sentTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("tradingStatus", FieldType.STRING).withNullable(true),
              Schema.Field.of("periodCode", FieldType.STRING).withNullable(true),
              Schema.Field.of("productCode", FieldType.STRING).withNullable(true),
              Schema.Field.of("productGroup", FieldType.STRING).withNullable(true),
              Schema.Field.of("productType", FieldType.STRING).withNullable(true),
              Schema.Field.of("symbol", FieldType.STRING).withNullable(true),
              Schema.Field.of("askLevelLastUpdateTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("askLevelOrderCnt", FieldType.INT64).withNullable(true),
              Schema.Field.of("askLevelPrice", FieldType.INT64).withNullable(true),
              Schema.Field.of("askLevelQty", FieldType.INT64).withNullable(true),
              Schema.Field.of("bidLevelLastUpdateTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("bidLevelOrderCnt", FieldType.INT64).withNullable(true),
              Schema.Field.of("bidLevelPrice", FieldType.INT64).withNullable(true),
              Schema.Field.of("bidLevelQty", FieldType.INT64).withNullable(true))
          .collect(toSchema());

  /**
   * Schema to extract Trade Info following attributes to convert to TSDataPoint <br>
   * <br>
   * sentTime, lastUpdateTime, tradePrice, tradeQty, tradeOrderCount, tradeUpdateAction, periodCode,
   * productCode, productType, productGroup, symbol
   */
  public static final Schema symbolTradeInfo =
      Stream.of(
              Schema.Field.of("sentTime", FieldType.STRING).withNullable(true),
              Schema.Field.of("lastUpdateTime", FieldType.INT64).withNullable(true),
              Schema.Field.of("tradePrice", FieldType.INT64).withNullable(true),
              Schema.Field.of("tradeQty", FieldType.INT64).withNullable(true),
              Schema.Field.of("tradeOrderCount", FieldType.INT64).withNullable(true),
              Schema.Field.of("tradeUpdateAction", FieldType.STRING).withNullable(true),
              Schema.Field.of("periodCode", FieldType.STRING).withNullable(true),
              Schema.Field.of("productCode", FieldType.STRING).withNullable(true),
              Schema.Field.of("productType", FieldType.STRING).withNullable(true),
              Schema.Field.of("productGroup", FieldType.STRING).withNullable(true),
              Schema.Field.of("symbol", FieldType.STRING).withNullable(true))
          .collect(toSchema());
}
