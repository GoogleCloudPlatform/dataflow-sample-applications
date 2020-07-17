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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.stock;

import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.businesslogic.core.utils.JSONUtils;
import com.google.dataflow.sample.retail.businesslogic.core.utils.Print;
import com.google.dataflow.sample.retail.businesslogic.core.utils.WriteRawJSONMessagesToBigQuery;
import com.google.dataflow.sample.retail.dataobjects.Stock.StockEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class StockProcessing extends PTransform<PCollection<String>, PCollection<StockEvent>> {

  @Override
  public PCollection<StockEvent> expand(PCollection<String> input) {
    Pipeline p = input.getPipeline();

    RetailPipelineOptions options = p.getOptions().as(RetailPipelineOptions.class);

    /**
     * **********************************************************************************************
     * Write Raw Inventory delivery
     * **********************************************************************************************
     */
    input.apply(new WriteRawJSONMessagesToBigQuery(options.getInventoryBigQueryRawTable()));
    /**
     * **********************************************************************************************
     * Validate Inventory delivery
     * **********************************************************************************************
     */
    PCollection<StockEvent> inventory =
        input.apply(JSONUtils.ConvertJSONtoPOJO.create(StockEvent.class));

    /**
     * **********************************************************************************************
     * Write Cleaned Data to BigQuery
     * **********************************************************************************************
     */
    if (options.getTestModeEnabled()) {
      input.apply(ParDo.of(new Print<>()));

    } else {
      inventory.apply(
          "StoreCorrectedInventoryDataToDW",
          BigQueryIO.<StockEvent>write()
              .useBeamSchema()
              .withWriteDisposition(WriteDisposition.WRITE_APPEND)
              //     .withTimePartitioning(new TimePartitioning().setField("timestamp"))
              .to(
                  String.format(
                      "%s:%s",
                      options.getDataWarehouseOutputProject(),
                      options.getInventoryBigQueryCleanTable())));
    }

    return inventory.apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));
  }
}
