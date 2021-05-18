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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.util.Timestamps;
import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts {@link Row} with schema {@link
 * com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.Data#symbolAskBid} into {@link
 * TSDataPoint}
 *
 * <ul>
 *   <li>Stamping Categorical TSDataPoints with askLevelLastUpdateTime Timestamp
 *   <li>Stamping askLevel TSDataPoints with askLevelLastUpdateTime Timestamp
 *   <li>Stamping bidLevel TSDataPoints with askLevelLastUpdateTime Timestamp
 * </ul>
 */
class ConvertCMETOBRowToTSDataPoints
    extends PTransform<PCollection<Row>, PCollection<TSDataPoint>> {

  public static final Logger LOG = LoggerFactory.getLogger(ConvertCMETOBRowToTSDataPoints.class);

  @Override
  public PCollection<TSDataPoint> expand(PCollection<Row> input) {

    return input.apply(ParDo.of(new ConvertRowToTSDataPoint()));
  }

  private static class ConvertRowToTSDataPoint extends DoFn<Row, TSDataPoint> {

    @ProcessElement
    public void process(@Element Row element, OutputReceiver<TSDataPoint> out) {

      Long sentTime = element.getInt64("sentTime");

      String tradingStatus = element.getString("tradingStatus");
      String periodCode = element.getString("periodCode");
      String productCode = element.getString("productCode");
      String productGroup = element.getString("productGroup");
      String productType = element.getString("productType");
      String symbol = element.getString("symbol");

      Long askLevelLastUpdateTime = element.getInt64("askLevelLastUpdateTime");
      Long askLevelOrderCnt = element.getInt64("askLevelOrderCnt");
      Long askLevelPrice = element.getInt64("askLevelPrice");
      Long askLevelQty = element.getInt64("askLevelQty");

      Long bidLevelLastUpdateTime = element.getInt64("bidLevelLastUpdateTime");
      Long bidLevelOrderCnt = element.getInt64("bidLevelOrderCnt");
      Long bidLevelPrice = element.getInt64("bidLevelPrice");
      Long bidLevelQty = element.getInt64("bidLevelQty");

      Map<String, String> metadataMap = new HashMap<>();

      metadataMap.put("sentTime", String.valueOf(sentTime));
      metadataMap.put("askLevelLastUpdateTime", String.valueOf(askLevelLastUpdateTime));
      metadataMap.put("bidLevelLastUpdateTime", String.valueOf(bidLevelLastUpdateTime));

      if (symbol != null) {
        // Stamping Categorical TSDataPoints with askLevelLastUpdateTime Timestamp

        if (askLevelLastUpdateTime != null) {
          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("tradingStatus"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(Data.newBuilder().setCategoricalVal(tradingStatus).build())
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("periodCode"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(Data.newBuilder().setCategoricalVal(periodCode).build())
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("productCode"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(Data.newBuilder().setCategoricalVal(productCode).build())
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("productGroup"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(Data.newBuilder().setCategoricalVal(productGroup).build())
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("productType"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(Data.newBuilder().setCategoricalVal(productType).build())
                  .putAllMetadata(metadataMap)
                  .build());
        }

        if (askLevelLastUpdateTime != null) {
          // Stamping askLevel TSDataPoints with askLevelLastUpdateTime Timestamp

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("askOrderCnt"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(CommonUtils.createNumData(askLevelOrderCnt))
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("askPrice"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(CommonUtils.createNumData(askLevelPrice))
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("askQty"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(CommonUtils.createNumData(askLevelQty))
                  .putAllMetadata(metadataMap)
                  .build());
        }

        if (askLevelLastUpdateTime != null) {
          // Stamping bidLevel TSDataPoints with askLevelLastUpdateTime Timestamp

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("bidOrderCnt"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(CommonUtils.createNumData(bidLevelOrderCnt))
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("bidPrice"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(CommonUtils.createNumData(bidLevelPrice))
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("bidQty"))
                  .setTimestamp(Timestamps.fromNanos(askLevelLastUpdateTime))
                  .setData(CommonUtils.createNumData(bidLevelQty))
                  .putAllMetadata(metadataMap)
                  .build());
        }
      } else {
        LOG.info(String.format("Timestamp or Value was Null, not processing. %s", element));
      }
    }
  }
}
