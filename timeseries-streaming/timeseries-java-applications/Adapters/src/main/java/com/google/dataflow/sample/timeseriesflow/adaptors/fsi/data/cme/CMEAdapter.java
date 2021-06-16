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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.Extract.ExtractAskBidDoFn;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.Extract.ExtractTradeInfoDoFn;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.GenerateErrorMessageEvents.GenerateGenericErrorMessage;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TopOfBook.TopOfBookEvent;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.TradeInfo.TradeInfoEvent;
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.Util.ErrorMessage;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Adapter class for <a
 * href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON">CME
 * Smart Stream on GCP JSON</a> data feeds
 */
@Experimental
public class CMEAdapter {

  public CMEAdapter() {}

  /**
   * CME Smart Stream Cloud Link - <a
   * href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON#CMESmartStreamonGCPJSON-BookMessage">Top
   * Of Book JSON </a> Transform
   *
   * <p>Converts {@link TopOfBookEvent} into {@link TSDataPoint}
   *
   * <ul>
   *   <li>Input Timestamps are assumed to be in Central Time Zone, so done the required conversions
   *       in the class
   * </ul>
   *
   * <ul>
   *   <li>Stamping Categorical TSDataPoints with askLevelLastUpdateTime Timestamp
   *   <li>Stamping askLevel TSDataPoints with askLevelLastUpdateTime Timestamp
   *   <li>Stamping bidLevel TSDataPoints with askLevelLastUpdateTime Timestamp
   * </ul>
   *
   * <p>Sink type to store Dead Letter records, must be either {@link DeadLetterSink#LOG} or {@link
   * DeadLetterSink#BIGQUERY}
   *
   * <p>When specifying {@link DeadLetterSink#BIGQUERY} , the {@link BigQueryIO#write()} is used
   * with following options
   *
   * <ul>
   *   <li>WRITE_APPEND
   *   <li>CREATE_IF_NEEDED
   * </ul>
   */
  @AutoValue
  public abstract static class SSCLTOBJsonTransform
      extends PTransform<PCollection<String>, PCollection<TSDataPoint>> {

    public static final Logger LOG = LoggerFactory.getLogger(SSCLTOBJsonTransform.class);

    @Nullable
    public abstract DeadLetterSink getDeadLetterSinkType();

    public abstract Boolean getSuppressCategorical();

    public abstract Boolean getUseSourceTimeMetadata();

    @Nullable
    public abstract String getBigQueryDeadLetterSinkProject();

    @Nullable
    public abstract String getBigQueryDeadLetterSinkTable();

    public abstract Builder toBuilder();

    public static Builder newBuilder() {
      return new AutoValue_CMEAdapter_SSCLTOBJsonTransform.Builder()
          .setSuppressCategorical(false)
          .setUseSourceTimeMetadata(false);
    }

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * Sink type for Dead Letter records
       *
       * @param value sink type to store Dead Letter records, must be either {@link
       *     DeadLetterSink#LOG} or {@link DeadLetterSink#BIGQUERY}
       * @return SSCLTOBJsonTransform.Builder
       */
      public abstract Builder setDeadLetterSinkType(DeadLetterSink value);

      /**
       * Must be set when specifying {@link DeadLetterSink#BIGQUERY} as the value of
       * deadLetterSinkType
       *
       * @param value GCP BigQuery Project ID
       * @return SSCLTOBJsonTransform.Builder
       */
      public abstract Builder setBigQueryDeadLetterSinkProject(String value);

      /**
       * Must be set when specifying {@link DeadLetterSink#BIGQUERY} as the value of
       * deadLetterSinkType
       *
       * @param value GCP BigQuery Table ID of format "datasetId.tableId"
       * @return SSCLTOBJsonTransform.Builder
       */
      public abstract Builder setBigQueryDeadLetterSinkTable(String value);

      /** Suppress output of Categorical values. */
      public abstract Builder setSuppressCategorical(Boolean value);

      /** Override the element timestamp with the source timestamp */
      public abstract Builder setUseSourceTimeMetadata(Boolean value);

      abstract SSCLTOBJsonTransform autoBuild();

      public SSCLTOBJsonTransform build() {
        SSCLTOBJsonTransform sscltobJsonTransform = autoBuild();
        if (sscltobJsonTransform.getDeadLetterSinkType() == null
            || !(sscltobJsonTransform.getDeadLetterSinkType().equals(DeadLetterSink.LOG)
                || sscltobJsonTransform.getDeadLetterSinkType().equals(DeadLetterSink.BIGQUERY))) {
          throw new IllegalArgumentException(
              "deadLetterSinkType must be supplied a value of either 'LOG' or 'BIGQUERY'");
        } else if (sscltobJsonTransform.getDeadLetterSinkType().equals(DeadLetterSink.BIGQUERY)) {
          if (sscltobJsonTransform.getBigQueryDeadLetterSinkProject() == null
              || sscltobJsonTransform.getBigQueryDeadLetterSinkTable() == null) {
            throw new IllegalArgumentException(
                "deadLetterSinkType is specified as 'BIGQUERY'. Must also specify values for "
                    + "bigQueryDeadLetterSinkProject and bigQueryDeadLetterSinkTable");
          }
        }
        return sscltobJsonTransform;
      }
    }

    @Override
    public PCollection<TSDataPoint> expand(PCollection<String> input) {

      Schema tobInputSchema = null;

      try {
        tobInputSchema = input.getPipeline().getSchemaRegistry().getSchema(TopOfBookEvent.class);
      } catch (NoSuchSchemaException e) {
        LOG.error(e.getMessage());
        throw new IllegalArgumentException(
            String.format(
                "Could not find schema for Object of %s", TopOfBookEvent.class.getCanonicalName()));
      }

      Schema errorMessageSchema = null;

      try {
        errorMessageSchema = input.getPipeline().getSchemaRegistry().getSchema(ErrorMessage.class);
      } catch (NoSuchSchemaException e) {
        LOG.error(e.getMessage());
        throw new IllegalArgumentException(
            String.format("Could not find schema for %s", ErrorMessage.class.getCanonicalName()));
      }

      // Convert JSON to Top of Book Row Schema
      PCollection<Row> tobRow =
          input.apply(
              "Convert to TOB Row",
              CMEMarketDataReader.newBuilder()
                  .setSchema(tobInputSchema)
                  .setDeadLetterSinkType(getDeadLetterSinkType())
                  .setBigQueryDeadLetterSinkProject(getBigQueryDeadLetterSinkProject())
                  .setBigQueryDeadLetterSinkTable(getBigQueryDeadLetterSinkTable())
                  .build());

      // Extract Ask & Bid and other key elements
      PCollectionTuple askBidRow =
          tobRow
              .apply("Convert to TopOfBook", Convert.fromRows(TopOfBookEvent.class))
              .apply(
                  "Extract Ask & Bid with Key Elements",
                  ParDo.of(new ExtractAskBidDoFn())
                      .withOutputTags(
                          Util.extractAskBidSuccess, TupleTagList.of(Util.extractAskBidFailure)));

      // Convert Extracted Ask Bid Row Schema to TSDataPoints

      PCollection<TSDataPoint> tobTSDataPoint =
          askBidRow
              .get(Util.extractAskBidSuccess)
              .setRowSchema(Data.symbolAskBid)
              .apply("Convert to TSDataPoints", new ConvertCMETOBRowToTSDataPoints(this));

      // Create ErrorMessage events for null data points (Failed records from ExtractAskBidDoFn)
      PCollection<ErrorMessage> errorMessages =
          askBidRow
              .get(Util.extractAskBidFailure)
              .apply(
                  "Error Message Events",
                  ParDo.of(
                      new GenerateGenericErrorMessage<TopOfBookEvent>(
                          errorMessageSchema,
                          "Either Found null for either field 'askLevel.price' or field 'bidLevel"
                              + ".price' or Timestamp parsing error for 'sentTime', 'askLevel"
                              + ".lastUpdateTime', 'bidLevel.lastUpdateTime")))
              .setCoder(SerializableCoder.of(Row.class))
              .setRowSchema(errorMessageSchema)
              .apply("Error Messages to Row", Convert.fromRows(ErrorMessage.class));

      if (Objects.equals(getDeadLetterSinkType(), DeadLetterSink.LOG)) {
        errorMessages.apply("Log ErrorMessages", new LogElements<ErrorMessage>());
      } else if (Objects.equals(getDeadLetterSinkType(), DeadLetterSink.BIGQUERY)) {
        errorMessages.apply(
            BigQueryIO.<ErrorMessage>write()
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .to(getBigQueryDeadLetterSinkProject() + ":" + getBigQueryDeadLetterSinkTable())
                .useBeamSchema());
      }

      return tobTSDataPoint;
    }
  }

  /**
   * CME Smart Stream Cloud Link - <a
   * href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON#CMESmartStreamonGCPJSON-TradeMessage">Trade
   * Info JSON </a> Transform
   *
   * <p>Converts {@link TradeInfoEvent} into {@link TSDataPoint}
   *
   * <ul>
   *   <li>Input Timestamps are assumed to be in Central Time Zone, so done the required conversions
   *       in the class
   * </ul>
   *
   * <ul>
   *   <li>Stamping Categorical TSDataPoints with lastUpdateTime Timestamp
   *   <li>Stamping trade info details TSDataPoints with lastUpdateTime Timestamp
   * </ul>
   *
   * <p>Sink type to store Dead Letter records, must be either {@link DeadLetterSink#LOG} or {@link
   * DeadLetterSink#BIGQUERY}
   *
   * <p>When specifying {@link DeadLetterSink#BIGQUERY} , the {@link BigQueryIO#write()} is used
   * with following options
   *
   * <ul>
   *   <li>WRITE_APPEND
   *   <li>CREATE_IF_NEEDED
   * </ul>
   */
  @AutoValue
  public abstract static class SSCLTRDJsonTransform
      extends PTransform<PCollection<String>, PCollection<TSDataPoint>> {

    public static final Logger LOG = LoggerFactory.getLogger(SSCLTRDJsonTransform.class);

    @Nullable
    public abstract DeadLetterSink getDeadLetterSinkType();

    public abstract Boolean getSuppressCategorical();

    public abstract Boolean getUseSourceTimeMetadata();

    @Nullable
    public abstract String getBigQueryDeadLetterSinkProject();

    @Nullable
    public abstract String getBigQueryDeadLetterSinkTable();

    public abstract Builder toBuilder();

    public static Builder newBuilder() {
      return new AutoValue_CMEAdapter_SSCLTRDJsonTransform.Builder()
          .setSuppressCategorical(false)
          .setUseSourceTimeMetadata(false);
    }

    @AutoValue.Builder
    public abstract static class Builder {

      /**
       * Sink type for Dead Letter records
       *
       * @param value sink type to store Dead Letter records, must be either {@link
       *     DeadLetterSink#LOG} or {@link DeadLetterSink#BIGQUERY}
       * @return SSCLTSJsonTransform.Builder
       */
      public abstract Builder setDeadLetterSinkType(DeadLetterSink value);

      /**
       * Must be set when specifying {@link DeadLetterSink#BIGQUERY} as the value of
       * deadLetterSinkType
       *
       * @param value GCP BigQuery Project ID
       * @return SSCLTSJsonTransform.Builder
       */
      public abstract Builder setBigQueryDeadLetterSinkProject(String value);

      /** Suppress output of Categorical values. */
      public abstract Builder setSuppressCategorical(Boolean value);

      /** Override the element timestamp with the source timestamp */
      public abstract Builder setUseSourceTimeMetadata(Boolean value);

      /**
       * Must be set when specifying {@link DeadLetterSink#BIGQUERY} as the value of
       * deadLetterSinkType
       *
       * @param value GCP BigQuery Table ID of format "datasetId.tableId"
       * @return SSCLTSJsonTransform.Builder
       */
      public abstract Builder setBigQueryDeadLetterSinkTable(String value);

      abstract SSCLTRDJsonTransform autoBuild();

      public SSCLTRDJsonTransform build() {
        SSCLTRDJsonTransform sscltrdJsonTransform = autoBuild();
        if (sscltrdJsonTransform.getDeadLetterSinkType() == null
            || !(sscltrdJsonTransform.getDeadLetterSinkType().equals(DeadLetterSink.LOG)
                || sscltrdJsonTransform.getDeadLetterSinkType().equals(DeadLetterSink.BIGQUERY))) {
          throw new IllegalArgumentException(
              "deadLetterSinkType must be supplied a value of either 'LOG' or 'BIGQUERY'");
        } else if (sscltrdJsonTransform.getDeadLetterSinkType().equals(DeadLetterSink.BIGQUERY)) {
          if (sscltrdJsonTransform.getBigQueryDeadLetterSinkProject() == null
              || sscltrdJsonTransform.getBigQueryDeadLetterSinkTable() == null) {
            throw new IllegalArgumentException(
                "deadLetterSinkType is specified as 'BIGQUERY'. Must also specify values for "
                    + "bigQueryDeadLetterSinkProject and bigQueryDeadLetterSinkTable");
          }
        }
        return sscltrdJsonTransform;
      }
    }

    @Override
    public PCollection<TSDataPoint> expand(PCollection<String> input) {

      Schema tsInputSchema = null;
      try {
        tsInputSchema = input.getPipeline().getSchemaRegistry().getSchema(TradeInfoEvent.class);
      } catch (NoSuchSchemaException e) {
        LOG.error(e.getMessage());
        throw new IllegalArgumentException(
            String.format(
                "Could not find schema for Object of %s", TradeInfoEvent.class.getCanonicalName()));
      }

      Schema errorMessageSchema = null;

      try {
        errorMessageSchema = input.getPipeline().getSchemaRegistry().getSchema(ErrorMessage.class);
      } catch (NoSuchSchemaException e) {
        LOG.error(e.getMessage());
        throw new IllegalArgumentException(
            String.format("Could not find schema for %s", ErrorMessage.class.getCanonicalName()));
      }

      // Convert JSON to Trade Info Row Schema
      PCollection<Row> tobRow =
          input.apply(
              "Convert to TS Row",
              CMEMarketDataReader.newBuilder()
                  .setSchema(tsInputSchema)
                  .setDeadLetterSinkType(getDeadLetterSinkType())
                  .setBigQueryDeadLetterSinkProject(getBigQueryDeadLetterSinkProject())
                  .setBigQueryDeadLetterSinkTable(getBigQueryDeadLetterSinkTable())
                  .build());

      // Extract Trade Information key elements
      PCollectionTuple tradeInfo =
          tobRow
              .apply("Convert to TradeInfo", Convert.fromRows(TradeInfoEvent.class))
              .apply(
                  "Extract Trade Info Key Elements",
                  ParDo.of(new ExtractTradeInfoDoFn())
                      .withOutputTags(
                          Util.extractTradeInfoSuccess,
                          TupleTagList.of(Util.extractTradeInfoFailure)));

      // Convert Extracted Trade Info Row Schema to TSDataPoints
      PCollection<TSDataPoint> trdTSDataPoint =
          tradeInfo
              .get(Util.extractTradeInfoSuccess)
              .setRowSchema(Data.symbolTradeInfo)
              .apply("Convert to TSDataPoints", new ConvertCMETRDRowToTSDataPoints(this));

      // Create ErrorMessage events for null data points (Failed records from ExtractTradeInfoDoFn)
      PCollection<ErrorMessage> errorMessages =
          tradeInfo
              .get(Util.extractTradeInfoFailure)
              .apply(
                  "Error Message Events",
                  ParDo.of(
                      new GenerateGenericErrorMessage<TradeInfoEvent>(
                          errorMessageSchema,
                          "Found null for either lastUpdateTime, tradePrice, "
                              + "tradeQty or tradeOrderCount")))
              .setCoder(SerializableCoder.of(Row.class))
              .setRowSchema(errorMessageSchema)
              .apply("Error Messages to Row", Convert.fromRows(ErrorMessage.class));

      if (Objects.equals(getDeadLetterSinkType(), DeadLetterSink.LOG)) {
        errorMessages.apply("Log ErrorMessages", new LogElements<ErrorMessage>());
      } else if (Objects.equals(getDeadLetterSinkType(), DeadLetterSink.BIGQUERY)) {
        errorMessages.apply(
            BigQueryIO.<ErrorMessage>write()
                .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .to(getBigQueryDeadLetterSinkProject() + ":" + getBigQueryDeadLetterSinkTable())
                .useBeamSchema());
      }

      return trdTSDataPoint;
    }
  }
}
