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
import com.google.dataflow.sample.retail.businesslogic.core.transforms.CreateStockAggregatorMetadata;
import com.google.dataflow.sample.retail.businesslogic.core.utils.JSONUtils;
import com.google.dataflow.sample.retail.businesslogic.core.utils.ReadPubSubMsgPayLoadAsString;
import com.google.dataflow.sample.retail.businesslogic.core.utils.WriteRawJSONMessagesToBigQuery;
import com.google.dataflow.sample.retail.dataobjects.Stock.StockEvent;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class Stock {

  public static PCollection<StockEvent> processStockPipeline(Pipeline p) {

    /**
     * **********************************************************************************************
     * Read Inventory delivery
     * **********************************************************************************************
     */
    RetailPipelineOptions options = p.getOptions().as(RetailPipelineOptions.class);

    PCollection<String> inventoryJSON =
        p.apply(
            "ReadStockStream",
            new ReadPubSubMsgPayLoadAsString(options.getInventoryPubSubSubscriptions()));

    return inventoryJSON.apply("ProcessStock", new ProcessStock());
  }

  private static class ProcessStock
      extends PTransform<PCollection<String>, PCollection<StockEvent>> {

    @Override
    public PCollection<StockEvent> expand(PCollection<String> input) {
      Pipeline p = input.getPipeline();

      RetailPipelineOptions options = p.getOptions().as(RetailPipelineOptions.class);

      /** Collect schemas to be used in this pipeline */
      Schema inventorySchema = null;

      try {
        p.getSchemaRegistry().registerPOJO(StockEvent.class);

        inventorySchema = p.getSchemaRegistry().getSchema(StockEvent.class);
      } catch (NoSuchSchemaException e) {
        e.printStackTrace();
        try {
          throw e;
        } catch (NoSuchSchemaException ex) {
          ex.printStackTrace();
        }
      }

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

      return inventory.apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));
    }
  }

  public static class CountIncomingStockPerProductLocation
      extends PTransform<PCollection<StockEvent>, PCollection<StockAggregation>> {

    @Override
    public PCollection<StockAggregation> expand(PCollection<StockEvent> input) {
      return input
          .apply(Select.<StockEvent>fieldNames("productId", "storeId"))
          .apply(
              Group.<Row>byFieldNames("productId", "storeId")
                  .aggregateField("storeId", Count.combineFn(), "count"))
          .apply(Select.<Row>fieldNames("key.productId", "key.storeId", "value.count"))
          .apply(
              AddFields.<Row>create()
                  // Need this field until we have @Nullable Schema check
                  .field("durationMS", FieldType.INT64, 1L)
                  .field("startTime", FieldType.INT64, 1L))
          .apply(Convert.to(StockAggregation.class))
          .apply(new CreateStockAggregatorMetadata(Duration.standardSeconds(5).getMillis()));
    }
  }

  public static class CountGlobalStockFromLocationStock
      extends PTransform<PCollection<StockAggregation>, PCollection<StockAggregation>> {

    @Override
    public PCollection<StockAggregation> expand(PCollection<StockAggregation> input) {
      return input
          .apply(Select.<StockAggregation>fieldNames("productId"))
          .apply(
              Group.<Row>byFieldNames("productId")
                  .aggregateField("productId", Count.<Integer>combineFn(), "count"))
          .apply(Select.<Row>fieldNames("key.productId", "value.count"))
          .apply(
              AddFields.<Row>create()
                  // Need this field until we have @Nullable Schema check
                  .field("storeId", FieldType.INT32, 1)
                  .field("durationMS", FieldType.INT64, 1L)
                  .field("startTime", FieldType.INT64, 1L))
          .apply(Convert.to(StockAggregation.class))
          .apply(new CreateStockAggregatorMetadata(Duration.standardSeconds(5).getMillis()));
    }
  }
}
