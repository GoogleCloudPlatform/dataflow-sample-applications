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
import com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme.GenerateErrorMessageEvents.GenerateJsonToRowErrorMessage;
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
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.JsonToRow.ParseResult;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to read CME Smart Stream Market Data feed
 *
 * <p>Converts <a
 * href="https://www.cmegroup.com/confluence/display/EPICSANDBOX/CME+Smart+Stream+on+GCP+JSON#CMESmartStreamonGCPJSON-BookMessage">Top
 * Of Book JSON </a> into {@link TopOfBook.TopOfBookEvent}
 *
 * <p>When specifying {@link DeadLetterSink#BIGQUERY} , the {@link BigQueryIO#write()} is used with
 * following options
 *
 * <ul>
 *   <li>WRITE_APPEND
 *   <li>CREATE_IF_NEEDED
 * </ul>
 */
@AutoValue
@Experimental
abstract class CMEMarketDataReader extends PTransform<PCollection<String>, PCollection<Row>> {

  public static final Logger LOG = LoggerFactory.getLogger(CMEMarketDataReader.class);

  @Nullable
  public abstract Schema getSchema();

  @Nullable
  public abstract Class getDataClass();

  @Nullable
  public abstract DeadLetterSink getDeadLetterSinkType();

  @Nullable
  public abstract String getBigQueryDeadLetterSinkProject();

  @Nullable
  public abstract String getBigQueryDeadLetterSinkTable();

  public abstract Builder toBuilder();

  public static Builder newBuilder() {
    return new AutoValue_CMEMarketDataReader.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Schema to convert JSON events (Either DataClass or Schema must be set)
     *
     * @param value schema to convert JSON events into
     * @return Schema
     */
    public abstract Builder setSchema(Schema value);

    /**
     * Class of the JSON events (Either DataClass or Schema must be set)
     *
     * @param value to convert JSON events into
     * @return Class
     */
    public abstract Builder setDataClass(Class value);

    /**
     * Sink type for Dead Letter records
     *
     * @param value sink type to store Dead Letter records, must be either {@link
     *     DeadLetterSink#LOG} or {@link DeadLetterSink#BIGQUERY}
     * @return CMEMarketDataReader.Builder
     */
    public abstract Builder setDeadLetterSinkType(DeadLetterSink value);

    /**
     * Must be set when specifying {@link DeadLetterSink#BIGQUERY} as the value of
     * deadLetterSinkType
     *
     * @param value GCP BigQuery Project ID
     * @return CMEMarketDataReader.Builder
     */
    public abstract Builder setBigQueryDeadLetterSinkProject(String value);

    /**
     * Must be set when specifying {@link DeadLetterSink#BIGQUERY} as the value of
     * deadLetterSinkType
     *
     * @param value GCP BigQuery Table ID of format "datasetId.tableId"
     * @return CMEMarketDataReader.Builder
     */
    public abstract Builder setBigQueryDeadLetterSinkTable(String value);

    abstract CMEMarketDataReader autoBuild();

    public CMEMarketDataReader build() {
      CMEMarketDataReader cmeMarketDataReader = autoBuild();
      if (cmeMarketDataReader.getDeadLetterSinkType() == null
          || !(cmeMarketDataReader.getDeadLetterSinkType().equals(DeadLetterSink.LOG)
              || cmeMarketDataReader.getDeadLetterSinkType().equals(DeadLetterSink.BIGQUERY))) {
        throw new IllegalArgumentException(
            "deadLetterSinkType must be supplied a value of either 'LOG' or 'BIGQUERY'");
      }

      if (cmeMarketDataReader.getDeadLetterSinkType().equals(DeadLetterSink.BIGQUERY)) {
        if (cmeMarketDataReader.getBigQueryDeadLetterSinkProject() == null
            || cmeMarketDataReader.getBigQueryDeadLetterSinkTable() == null) {
          throw new IllegalArgumentException(
              "deadLetterSinkType is specified as 'BIGQUERY'. Must also specify values for "
                  + "bigQueryDeadLetterSinkProject and bigQueryDeadLetterSinkTable");
        }
      }

      if (cmeMarketDataReader.getSchema() == null && cmeMarketDataReader.getDataClass() == null) {
        throw new IllegalArgumentException("Either Schema or Dataclass must be set");
      }

      return cmeMarketDataReader;
    }
  }

  @Override
  public PCollection<Row> expand(PCollection<String> input) {

    Schema errorMessageSchema = null;

    try {
      errorMessageSchema = input.getPipeline().getSchemaRegistry().getSchema(ErrorMessage.class);
    } catch (NoSuchSchemaException e) {
      LOG.error(e.getMessage());
      throw new IllegalArgumentException(
          String.format("Could not find schema for %s", ErrorMessage.class.getCanonicalName()));
    }

    Schema schema = getSchema();

    if (schema == null) {
      try {
        errorMessageSchema = input.getPipeline().getSchemaRegistry().getSchema(getClass());
      } catch (NoSuchSchemaException e) {
        LOG.error(e.getMessage());
        throw new IllegalArgumentException(
            String.format("Could not find schema for %s", getClass().getCanonicalName()));
      }
    }

    ParseResult parseResult =
        input.apply(JsonToRow.withExceptionReporting(getSchema()).withExtendedErrorInfo());

    // Create ErrorMessage events for incompatible schema (Failed records from JsonToRow)
    PCollection<ErrorMessage> errorMessages =
        parseResult
            .getFailedToParseLines()
            .apply(
                "Error Message Events",
                ParDo.of(new GenerateJsonToRowErrorMessage(errorMessageSchema)))
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

    return parseResult.getResults();
  }
}
