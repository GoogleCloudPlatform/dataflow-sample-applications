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
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.CMEAdapter.SSCLTRDJsonTransform;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn.AccumVWAPBuilder;
import com.google.protobuf.util.Timestamps;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Converts {@link Row} with schema {@link
 * com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.Data#symbolTradeInfo} into {@link
 * TSDataPoint}
 *
 * <ul>
 *   <li>Stamping Categorical TSDataPoints with lastUpdateTime Timestamp
 *   <li>Stamping trade info details TSDataPoints with lastUpdateTime Timestamp
 * </ul>
 *
 * Suppress Categorical value output using {@link SSCLTRDJsonTransform#getSuppressCategorical()}
 *
 * <p>To make use of the source timestamp rather than the data timestamp use {@link
 * SSCLTRDJsonTransform#getUseSourceTimeMetadata()}.
 */
@Experimental
class ConvertCMETRDRowToTSDataPoints
    extends PTransform<PCollection<Row>, PCollection<TSDataPoint>> {

  public static final Logger LOG = LoggerFactory.getLogger(ConvertCMETRDRowToTSDataPoints.class);

  SSCLTRDJsonTransform sscltrdJsonTransform;

  public ConvertCMETRDRowToTSDataPoints(SSCLTRDJsonTransform sscltrdJsonTransform) {
    this.sscltrdJsonTransform = sscltrdJsonTransform;
  }

  public ConvertCMETRDRowToTSDataPoints(
      @Nullable @UnknownKeyFor @Initialized String name,
      SSCLTRDJsonTransform sscltrdJsonTransform) {
    super(name);
    this.sscltrdJsonTransform = sscltrdJsonTransform;
  }

  @Override
  public PCollection<TSDataPoint> expand(PCollection<Row> input) {

    return input.apply(ParDo.of(new ConvertRowToTSDataPoint(this)));
  }

  private static class ConvertRowToTSDataPoint extends DoFn<Row, TSDataPoint> {

    final ConvertCMETRDRowToTSDataPoints convertCMETRDRowToTSDataPoints;

    public ConvertRowToTSDataPoint(ConvertCMETRDRowToTSDataPoints convertCMETRDRowToTSDataPoints) {
      this.convertCMETRDRowToTSDataPoints = convertCMETRDRowToTSDataPoints;
    }

    @ProcessElement
    public void process(
        @Element Row element, @Timestamp Instant sourceTimestamp, OutputReceiver<TSDataPoint> out) {

      String sentTime = element.getString("sentTime");
      Long lastUpdateTime = element.getInt64("lastUpdateTime");

      Long tradePrice = element.getInt64("tradePrice");
      Long tradeQty = element.getInt64("tradeQty");
      Long tradeOrderCount = element.getInt64("tradeOrderCount");
      String tradeUpdateAction = element.getString("tradeUpdateAction");

      String periodCode = element.getString("periodCode");
      String productCode = element.getString("productCode");
      String productGroup = element.getString("productGroup");
      String productType = element.getString("productType");
      String symbol = element.getString("symbol");

      Long elementTimestamp = null;

      if (convertCMETRDRowToTSDataPoints.sscltrdJsonTransform.getUseSourceTimeMetadata()) {
        elementTimestamp = TimeUnit.MILLISECONDS.toNanos(sourceTimestamp.getMillis());
      } else {
        elementTimestamp = lastUpdateTime;
      }

      Map<String, String> metadataMap = new HashMap<>();

      metadataMap.put("sentTime", String.valueOf(sentTime));
      metadataMap.put("lastUpdateTime", String.valueOf(lastUpdateTime));

      if (symbol != null) {
        // Stamping Categorical TSDataPoints with lastUpdateTime Timestamp

        if (lastUpdateTime != null) {

          if (!convertCMETRDRowToTSDataPoints.sscltrdJsonTransform.getSuppressCategorical()) {

            out.output(
                TSDataPoint.newBuilder()
                    .setKey(
                        TSKey.newBuilder()
                            .setMajorKey(symbol)
                            .build()
                            .toBuilder()
                            .setMinorKeyString("tradeUpdateAction"))
                    .setTimestamp(Timestamps.fromNanos(elementTimestamp))
                    .setData(Data.newBuilder().setCategoricalVal(tradeUpdateAction).build())
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
                    .setTimestamp(Timestamps.fromNanos(elementTimestamp))
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
                    .setTimestamp(Timestamps.fromNanos(elementTimestamp))
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
                            .setMinorKeyString("productType"))
                    .setTimestamp(Timestamps.fromNanos(elementTimestamp))
                    .setData(Data.newBuilder().setCategoricalVal(productType).build())
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
                    .setTimestamp(Timestamps.fromNanos(elementTimestamp))
                    .setData(Data.newBuilder().setCategoricalVal(productGroup).build())
                    .putAllMetadata(metadataMap)
                    .build());
          }
        }

        if (lastUpdateTime != null) {
          // Stamping trade info details TSDataPoints with lastUpdateTime Timestamp

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("tradePrice"))
                  .setTimestamp(Timestamps.fromNanos(elementTimestamp))
                  .setData(CommonUtils.createNumData(tradePrice))
                  .putExtendedData(AccumVWAPBuilder.VOL, CommonUtils.createNumData(tradeQty))
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("tradeQty"))
                  .setTimestamp(Timestamps.fromNanos(elementTimestamp))
                  .setData(CommonUtils.createNumData(tradeQty))
                  .putAllMetadata(metadataMap)
                  .build());

          out.output(
              TSDataPoint.newBuilder()
                  .setKey(
                      TSKey.newBuilder()
                          .setMajorKey(symbol)
                          .build()
                          .toBuilder()
                          .setMinorKeyString("tradeOrderCount"))
                  .setTimestamp(Timestamps.fromNanos(elementTimestamp))
                  .setData(CommonUtils.createNumData(tradeOrderCount))
                  .putAllMetadata(metadataMap)
                  .build());
        }

      } else {
        LOG.info(String.format("Timestamp or Value was Null, not processing. %s", element));
      }
    }
  }
}
