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
package com.google.dataflow.sample.retail.businesslogic.core.utils;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.api.services.bigquery.model.TimePartitioning;
import com.google.common.collect.ImmutableList;
import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineReportingOptions;
import javax.annotation.Nullable;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.joda.time.Instant;

public class WriteRawJSONMessagesToBigQuery extends PTransform<PCollection<String>, PDone> {

  private String bigQueryTable;

  public WriteRawJSONMessagesToBigQuery(String bigQueryTable) {
    this.bigQueryTable = bigQueryTable;
  }

  public WriteRawJSONMessagesToBigQuery(@Nullable String name, String bigQueryTable) {
    super(name);
    this.bigQueryTable = bigQueryTable;
  }

  @Override
  public PDone expand(PCollection<String> input) {

    RetailPipelineReportingOptions options =
        input.getPipeline().getOptions().as(RetailPipelineReportingOptions.class);

    /**
     * **********************************************************************************************
     * Write the raw output to BigQuery. The JSON is preserved.
     * **********************************************************************************************
     */
    input.apply(
        BigQueryIO.<String>write()
            .to(String.format("%s:%s", options.getDataWarehouseOutputProject(), bigQueryTable))
            .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
            .withWriteDisposition(WriteDisposition.WRITE_APPEND)
            .withTimePartitioning(new TimePartitioning().setField("processed_timestamp"))
            .withSchema(
                new TableSchema()
                    .setFields(
                        ImmutableList.of(
                            new TableFieldSchema()
                                .setName("processed_timestamp")
                                .setType("TIMESTAMP"),
                            new TableFieldSchema().setName("json").setType("STRING"))))
            .withFormatFunction(
                (SerializableFunction<String, TableRow>)
                    input1 ->
                        new TableRow()
                            .set("json", input1)
                            .set("processed_timestamp", Instant.now().getMillis() / 1000)));

    return PDone.in(input.getPipeline());
  }
}
