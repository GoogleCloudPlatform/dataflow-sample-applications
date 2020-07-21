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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream;

import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.businesslogic.core.utils.Print;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Duration;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Write page view aggregation data to BigQuery.
 *
 * @param <T>
 */
@Experimental
public class WriteAggregationToBigQuery<T> extends PTransform<PCollection<T>, PDone> {

  private String aggregationName;
  private Duration aggregationDuration;

  private static final Logger LOG = LoggerFactory.getLogger(WriteAggregationToBigQuery.class);

  WriteAggregationToBigQuery(String aggregationName, Duration aggregationDuration) {
    this.aggregationName = aggregationName;
    this.aggregationDuration = aggregationDuration;
  }

  private WriteAggregationToBigQuery(
      @Nullable String name, String aggregationName, Duration aggregationDuration) {
    super(name);
    this.aggregationName = aggregationName;
    this.aggregationDuration = aggregationDuration;
  }

  public static <T> WriteAggregationToBigQuery<T> create(
      String aggregationName, Duration aggregationDuration) {
    return new WriteAggregationToBigQuery<T>(aggregationName, aggregationDuration);
  }

  private static String createDurationSuffix(Duration duration) {

    PeriodFormatter yearsAndMonths =
        new PeriodFormatterBuilder()
            .printZeroRarelyLast()
            .appendYears()
            .appendSuffix("Y")
            .appendMonths()
            .appendSuffix("M")
            .appendHours()
            .appendSuffix("H")
            .appendMinutes()
            .appendSuffix("M")
            .appendSeconds()
            .appendSuffix("S")
            .appendMillis()
            .appendSuffix("MS")
            .printZeroRarelyLast()
            .toFormatter();

    return yearsAndMonths.print(duration.toPeriod());
  }

  public static WriteAggregationToBigQuery writeAggregationToBigQuery(
      String aggregationName, Duration aggregationDuration) {
    return new WriteAggregationToBigQuery(aggregationName, aggregationDuration);
  }

  @Override
  public PDone expand(PCollection<T> input) {

    RetailPipelineOptions options =
        input.getPipeline().getOptions().as(RetailPipelineOptions.class);

    if (options.getTestModeEnabled()) {

      input.apply(ParDo.of(new Print<T>("Aggregation to BigQuery is: ")));

      return PDone.in(input.getPipeline());
    }
    input.apply(
        BigQueryIO.<T>write()
            .useBeamSchema()
            .to(
                String.format(
                    "%s:%s.%s_%s",
                    options.getDataWarehouseOutputProject(),
                    options.getAggregateBigQueryDataset(),
                    aggregationName,
                    createDurationSuffix(aggregationDuration)))
            // .withTimePartitioning(new TimePartitioning().setField("startTime"))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    return PDone.in(input.getPipeline());
  }
}
