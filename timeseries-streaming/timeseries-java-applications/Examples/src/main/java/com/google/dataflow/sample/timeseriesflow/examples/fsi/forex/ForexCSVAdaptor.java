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

import static com.google.dataflow.sample.timeseriesflow.examples.fsi.forex.HistoryForexReader.deadLetterTag;
import static com.google.dataflow.sample.timeseriesflow.examples.fsi.forex.HistoryForexReader.successfulParse;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.protobuf.util.Timestamps;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldName;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// This class is used for backtesting or bootstrap in batch mode.
// The primary usage of the time series library being for live stream mode processing.
public class ForexCSVAdaptor {

  // Instantiate Logger.
  private static final Logger LOG = LoggerFactory.getLogger(ForexCSVAdaptor.class);

  public ForexCSVAdaptor() {}

  public static class ConvertCSVForex extends PTransform<PCollection<String>, PCollectionTuple> {

    Set<String> tickers;

    public ConvertCSVForex(Set<String> tickers) {
      this.tickers = tickers;
    }

    public ConvertCSVForex(@Nullable String name, Set<String> tickers) {
      super(name);
      this.tickers = tickers;
    }

    @Override
    public PCollectionTuple expand(PCollection<String> input) {
      return input.apply(
          ParDo.of(new ParseCSV(this))
              .withOutputTags(successfulParse, TupleTagList.of(deadLetterTag)));
    }

    private static class ParseCSV extends DoFn<String, TSDataPoint> {

      ConvertCSVForex convertCSVForex;

      public ParseCSV(ConvertCSVForex convertCSVForex) {
        this.convertCSVForex = convertCSVForex;
      }

      ObjectReader objectReader;

      @Setup
      public void setup() {
        CsvMapper mapper = new CsvMapper();
        objectReader =
            mapper.readerFor(Forex.class).with(Forex.getCsvSchema().withColumnSeparator(','));
      }

      @ProcessElement
      public void process(@Element String input, MultiOutputReceiver multiOutputReceiver) {
        try {
          MappingIterator<Forex> object = objectReader.readValues(input);
          if (object.hasNext()) {
            Forex forex = null;
            try {
              forex = object.next();

              if (convertCSVForex.tickers.contains(forex.ticker)) {

                TSKey key = TSKey.newBuilder().setMajorKey(forex.ticker).build();

                DateTimeFormatter formatter =
                    DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
                // We need to use local time which includes time zone,
                // in order to use a particular formatter for the parser
                LocalDateTime ldt = LocalDateTime.parse(forex.timestamp, formatter);
                // Setting UTC using Reykjavik time zone
                ZonedDateTime zdt = ldt.atZone(ZoneId.of("Atlantic/Reykjavik"));

                long millis = zdt.toInstant().toEpochMilli();
                Instant instant = Instant.ofEpochMilli(millis);
                com.google.protobuf.Timestamp timestamp = Timestamps.fromMillis(millis);

                HashMap<String, Double> minorKeys = new HashMap<>();

                minorKeys.put("ASK", forex.ask);
                minorKeys.put("BID", forex.bid);
                minorKeys.put("ASK_VOLUME", forex.ask_volume);
                minorKeys.put("BID_VOLUME", forex.bid_volume);

                for (Map.Entry<String, Double> entry : minorKeys.entrySet()) {
                  String k = entry.getKey();
                  Double v = entry.getValue();
                  try {
                    multiOutputReceiver
                        .get(successfulParse)
                        .outputWithTimestamp(
                            TSDataPoint.newBuilder()
                                .setKey(key.toBuilder().setMinorKeyString(k))
                                .setTimestamp(timestamp)
                                .setData(CommonUtils.createNumData(v))
                                .build(),
                            instant);
                  } catch (Exception e) {
                    LOG.error(
                        String.format(
                            "Unable to build TSDataPoint with exception, adding to DeadLetter queue: %s",
                            e));
                    multiOutputReceiver.get(deadLetterTag).output(forex.toString());
                  }
                }
              }
            } catch (Exception e) {
              LOG.error(String.format("Unable to get next element: %s", e));
              ;
            }
          }
        } catch (IOException e) {
          LOG.error(String.format("Unable to read elements from input: %s", e));
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
