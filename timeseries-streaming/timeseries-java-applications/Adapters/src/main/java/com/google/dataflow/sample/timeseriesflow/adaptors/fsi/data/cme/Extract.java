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

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TopOfBook.Payload;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TopOfBook.PriceLevel;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TopOfBook.TopOfBookEvent;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TradeInfo.TradeInfoEvent;
import com.google.protobuf.util.Timestamps;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A class to extract the relevant information from the input parsed data feed */
@Experimental
class Extract {

  public Extract() {}

  private static ZonedDateTime getDateTime(String date) {

    ZonedDateTime zonedDateTime;
    ZoneId chicagoZone = ZoneId.of("America/Chicago");

    if (date.endsWith("Z")) {
      ZonedDateTime elementTime = ZonedDateTime.parse(date, DateTimeFormatter.ISO_DATE_TIME);
      zonedDateTime = elementTime.withZoneSameInstant(chicagoZone);
    } else {
      LocalDateTime elementTime = LocalDateTime.parse(date);
      zonedDateTime = ZonedDateTime.of(elementTime, chicagoZone);
    }

    return zonedDateTime;
  }

  /**
   * DoFn to extract selective attributes {@link Data#symbolAskBid} from {@link TopOfBookEvent}
   *
   * <ul>
   *   <li>Input Timestamps are assumed to be in Central Time Zone, so done the required conversions
   *       in the class
   * </ul>
   */
  public static class ExtractAskBidDoFn extends DoFn<TopOfBookEvent, Row> {

    public static final Logger LOG = LoggerFactory.getLogger(ExtractAskBidDoFn.class);

    @ProcessElement
    public void processElement(@Element TopOfBookEvent element, MultiOutputReceiver out) {

      Long sentTime = null;
      ZonedDateTime elementSentZonedDateTime = getDateTime(element.getHeader().getSentTime());

      try {
        // Formatting for CT
        sentTime =
            Timestamps.toNanos(
                Timestamps.parse(elementSentZonedDateTime.toString().split("\\[")[0]));

      } catch (ParseException e) {
        out.get(Util.extractAskBidFailure).output(element);
        return;
      }

      for (Payload payload : element.getPayload()) {

        String tradingStatus = payload.getTradingStatus();
        String periodCode = payload.getInstrument().getPeriodCode();
        String productCode = payload.getInstrument().getProductCode();
        String productGroup = payload.getInstrument().getProductGroup();
        String productType = payload.getInstrument().getProductType();
        String symbol = payload.getInstrument().getSymbol();
        Long askLevelLastUpdateTime = null;
        Long askLevelOrderCnt = null;
        Long askLevelPrice = null;
        Long askLevelQty = null;
        Long bidLevelLastUpdateTime = null;
        Long bidLevelOrderCnt = null;
        Long bidLevelPrice = null;
        Long bidLevelQty = null;

        boolean invalidPayloadRecord = FALSE;

        for (PriceLevel askLevel : payload.getAskLevel()) {

          if (askLevel.getLastUpdateTime() == null
              || askLevel.getOrderCnt() == null
              || askLevel.getPrice() == null
              || askLevel.getQty() == null) {
            invalidPayloadRecord = TRUE;
            out.get(Util.extractAskBidFailure).output(element);
          } else {

            ZonedDateTime elementAskLevelLastUpdateZonedDateTime =
                getDateTime(askLevel.getLastUpdateTime());
            try {
              // Formatting for CT
              askLevelLastUpdateTime =
                  Timestamps.toNanos(
                      Timestamps.parse(
                          elementAskLevelLastUpdateZonedDateTime.toString().split("\\[")[0]));
            } catch (ParseException e) {
              invalidPayloadRecord = TRUE;
              out.get(Util.extractAskBidFailure).output(element);
            }

            askLevelOrderCnt = askLevel.getOrderCnt();
            askLevelPrice = Long.valueOf(askLevel.getPrice());
            askLevelQty = askLevel.getQty();
          }
        }

        if (!invalidPayloadRecord) {
          for (PriceLevel bidLevel : payload.getBidLevel()) {

            if (bidLevel.getLastUpdateTime() == null
                || bidLevel.getOrderCnt() == null
                || bidLevel.getPrice() == null
                || bidLevel.getQty() == null) {
              invalidPayloadRecord = TRUE;
              out.get(Util.extractAskBidFailure).output(element);
            } else {

              ZonedDateTime elementBidLevelLastUpdateZonedDateTime =
                  getDateTime(bidLevel.getLastUpdateTime());

              try {
                // Formatting for CT
                bidLevelLastUpdateTime =
                    Timestamps.toNanos(
                        Timestamps.parse(
                            elementBidLevelLastUpdateZonedDateTime.toString().split("\\[")[0]));
              } catch (ParseException e) {
                invalidPayloadRecord = TRUE;
                out.get(Util.extractAskBidFailure).output(element);
              }

              bidLevelOrderCnt = bidLevel.getOrderCnt();
              bidLevelPrice = Long.valueOf(bidLevel.getPrice());
              bidLevelQty = bidLevel.getQty();
            }
          }
        }

        if (!invalidPayloadRecord) {
          Row outputRow =
              Row.withSchema(Data.symbolAskBid)
                  .addValues(
                      sentTime,
                      tradingStatus,
                      periodCode,
                      productCode,
                      productGroup,
                      productType,
                      symbol,
                      askLevelLastUpdateTime,
                      askLevelOrderCnt,
                      askLevelPrice,
                      askLevelQty,
                      bidLevelLastUpdateTime,
                      bidLevelOrderCnt,
                      bidLevelPrice,
                      bidLevelQty)
                  .build();

          out.get(Util.extractAskBidSuccess).output(outputRow);
        }
      }
    }
  }

  /**
   * DoFn to extract selective attributes {@link Data#symbolTradeInfo} from {@link TradeInfoEvent}
   *
   * <ul>
   *   <li>Input Timestamps are assumed to be in Central Time Zone, so done the required conversions
   *       in the class
   * </ul>
   */
  public static class ExtractTradeInfoDoFn extends DoFn<TradeInfoEvent, Row> {

    public static final Logger LOG = LoggerFactory.getLogger(ExtractTradeInfoDoFn.class);

    @ProcessElement
    public void processElement(@Element TradeInfoEvent element, MultiOutputReceiver out) {

      String sentTime = element.getHeader().getSentTime();

      for (TradeInfo.Payload payload : element.getPayload()) {

        boolean invalidPayloadRecord = FALSE;

        Long lastUpdateTime = null;

        Long tradePrice = null;
        Long tradeQty = null;
        Long tradeOrderCount = null;
        String tradeUpdateAction = payload.getTradeSummary().getTradeUpdateAction();

        String periodCode = payload.getInstrument().getPeriodCode();
        String productCode = payload.getInstrument().getProductCode();
        String productGroup = payload.getInstrument().getProductGroup();
        String productType = payload.getInstrument().getProductType();
        String symbol = payload.getInstrument().getSymbol();

        if (payload.getLastUpdateTime() == null
            || payload.getTradeSummary().getTradePrice() == null
            || payload.getTradeSummary().getTradeQty() == null
            || payload.getTradeSummary().getTradeOrderCount() == null) {
          out.get(Util.extractTradeInfoFailure).output(element);
        } else {

          ZonedDateTime payloadLastUpdateZonedDateTime = getDateTime(payload.getLastUpdateTime());

          try {
            // Formatting for CT
            lastUpdateTime =
                Timestamps.toNanos(
                    Timestamps.parse(payloadLastUpdateZonedDateTime.toString().split("\\[")[0]));
          } catch (ParseException e) {
            invalidPayloadRecord = TRUE;
            out.get(Util.extractTradeInfoFailure).output(element);
          }

          tradePrice = Long.valueOf(payload.getTradeSummary().getTradePrice());
          tradeQty = Long.valueOf(payload.getTradeSummary().getTradeQty());
          tradeOrderCount = Long.valueOf(payload.getTradeSummary().getTradeOrderCount());
        }

        if (!invalidPayloadRecord) {
          Row outputRow =
              Row.withSchema(Data.symbolTradeInfo)
                  .addValues(
                      sentTime,
                      lastUpdateTime,
                      tradePrice,
                      tradeQty,
                      tradeOrderCount,
                      tradeUpdateAction,
                      periodCode,
                      productCode,
                      productGroup,
                      productType,
                      symbol)
                  .build();

          out.get(Util.extractTradeInfoSuccess).output(outputRow);
        }
      }
    }
  }
}
