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
package com.google.dataflow.sample.timeseriesflow.examples.fsi.forex;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;

import static jdk.nashorn.internal.objects.Global.print;

// This class is used for backtesting or bootstrap in batch mode, we would normally implement for streaming workloads
public class ForexCSVAdaptor {

  public ForexCSVAdaptor() {}

  public static class ConvertCSVForex
      extends PTransform<PCollection<String>, PCollection<TSDataPoint>> {

    Set<String> tickers;

    public ConvertCSVForex(Set<String> tickers) {
      this.tickers = tickers;
    }

    public ConvertCSVForex(@Nullable String name, Set<String> tickers) {
      super(name);
      this.tickers = tickers;
    }

    @Override
    public PCollection<TSDataPoint> expand(PCollection<String> input) {
      return input.apply(ParDo.of(new ParseCSV(this)));
    }

    private static class ParseCSV extends DoFn<String, TSDataPoint> {

      ConvertCSVForex convertCSVForex;

      public ParseCSV(ConvertCSVForex convertCSVForex) {
        this.convertCSVForex = convertCSVForex;
      }

      ObjectReader objectReader;

      @StartBundle
      public void startBundle() {
        CsvMapper mapper = new CsvMapper();
        objectReader =
            mapper.readerFor(Forex.class).with(Forex.getCsvSchema().withColumnSeparator(','));
      }

      @ProcessElement
      public void process(@Element String input, OutputReceiver<TSDataPoint> outputReceiver) {
        try {
          MappingIterator<Forex> object = objectReader.readValues(input);
          if (object.hasNext()) {
            Forex forex = null;
            try {
              forex = object.next();

              if (convertCSVForex.tickers.contains(forex.ticker)) {

                TSKey key = TSKey.newBuilder().setMajorKey(forex.ticker).build();

                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

                LocalDateTime ldt = LocalDateTime.parse(forex.timestamp, formatter);
                // Setting UTC using Reykjavik time zone
                ZonedDateTime zdt = ldt.atZone(ZoneId.of("Atlantic/Reykjavik"));

                long millis = zdt.toInstant().toEpochMilli();

                // OUTPUT Ask Price
                outputReceiver.outputWithTimestamp(
                    TSDataPoint.newBuilder()
                        .setKey(key.toBuilder().setMinorKeyString("ASK"))
                        .setTimestamp(Timestamps.fromMillis(millis))
                        .setData(CommonUtils.createNumData(forex.ask))
                        .build(), Instant.ofEpochMilli(millis));

                // OUTPUT Bid Price
                outputReceiver.outputWithTimestamp(
                    TSDataPoint.newBuilder()
                        .setKey(key.toBuilder().setMinorKeyString("BID"))
                        .setTimestamp(Timestamps.fromMillis(millis))
                        .setData(CommonUtils.createNumData(forex.bid))
                        .build(), Instant.ofEpochMilli(millis));

                // OUTPUT Ask volume
                outputReceiver.outputWithTimestamp(
                    TSDataPoint.newBuilder()
                        .setKey(key.toBuilder().setMinorKeyString("ASK_VOLUME"))
                        .setTimestamp(Timestamps.fromMillis(millis))
                        .setData(CommonUtils.createNumData(forex.ask_volume))
                        .build(), Instant.ofEpochMilli(millis));

                // OUTPUT Bid volume
                outputReceiver.outputWithTimestamp(
                    TSDataPoint.newBuilder()
                        .setKey(key.toBuilder().setMinorKeyString("BID_VOLUME"))
                        .setTimestamp(Timestamps.fromMillis(millis))
                        .setData(CommonUtils.createNumData(forex.bid_volume))
                        .build(), Instant.ofEpochMilli(millis));
              }
            } catch (Exception ex) {
              System.out.println(String.format("Unable to parse record: %s", ex));
            }
          }

        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }

  /**
   * ticker|timestamp|ask|bid|ask_volume|bid_volume Intermediary POJO used for conversion of CSV
   * file.
   */
  @DefaultSchema(JavaFieldSchema.class)
  public static class Forex {

    public Forex() {}

    public String ticker;

    @SchemaFieldName("timestamp")
    public String timestamp;

    @SchemaFieldName("ask")
    public Double ask;

    @SchemaFieldName("bid")
    public Double bid;

    @SchemaFieldName("ask_volume")
    public Double ask_volume;

    @SchemaFieldName("bid_volume")
    public Double bid_volume;

    public static CsvSchema getCsvSchema() {
      return CsvSchema.builder()
          .addColumn("ticker")
          .addColumn("timestamp")
          .addColumn("ask")
          .addColumn("bid")
          .addColumn("ask_volume")
          .addColumn("bid_volume")
          .build();
    }
  }
}
