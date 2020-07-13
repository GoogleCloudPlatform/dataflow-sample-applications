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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.transaction;

import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.CreateStockAggregatorMetadata;
import com.google.dataflow.sample.retail.businesslogic.core.utils.JSONUtils;
import com.google.dataflow.sample.retail.businesslogic.core.utils.ReadPubSubMsgPayLoadAsString;
import com.google.dataflow.sample.retail.businesslogic.core.utils.WriteRawJSONMessagesToBigQuery;
import com.google.dataflow.sample.retail.businesslogic.externalservices.SlowMovingStoreLocationDimension.StoreLocations;
import com.google.dataflow.sample.retail.dataobjects.Dimensions.StoreLocation;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import com.google.dataflow.sample.retail.dataobjects.Transaction.TransactionEvent;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.RenameFields;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

public class TransactionProcessing {

  public static PCollection<TransactionEvent> processTransactions(Pipeline p) {

    RetailPipelineOptions options = p.getOptions().as(RetailPipelineOptions.class);

    /**
     * **********************************************************************************************
     * Read Raw Transactions
     * **********************************************************************************************
     */
    PCollection<String> transactionsJSON =
        p.apply(
            "ReadTransactionStream",
            new ReadPubSubMsgPayLoadAsString(options.getTransactionsPubSubSubscription()));

    return transactionsJSON.apply("ProcessTransactions", new ProcessTransactions());
  }

  private static class ProcessTransactions
      extends PTransform<PCollection<String>, PCollection<TransactionEvent>> {

    @Override
    public PCollection<TransactionEvent> expand(PCollection<String> input) {

      RetailPipelineOptions options =
          input.getPipeline().getOptions().as(RetailPipelineOptions.class);

      /**
       * **********************************************************************************************
       * Write Raw Transactions
       * **********************************************************************************************
       */
      input.apply(new WriteRawJSONMessagesToBigQuery(options.getTransactionsBigQueryRawTable()));

      /**
       * **********************************************************************************************
       * Convert to Transactions Object
       * **********************************************************************************************
       */
      PCollection<TransactionEvent> transactions =
          input.apply(JSONUtils.ConvertJSONtoPOJO.create(TransactionEvent.class));

      /**
       * **********************************************************************************************
       * Validate & Enrich Transactions
       * **********************************************************************************************
       */
      PCollectionView<Map<Integer, StoreLocation>> storeLocationSideinput =
          input
              .getPipeline()
              .apply(
                  StoreLocations.create(
                      Duration.standardMinutes(10), options.getStoreLocationBigQueryTableRef()));

      PCollection<TransactionEvent> transactionWithStoreLoc =
          transactions.apply(EnrichTransactionWithStoreLocation.create(storeLocationSideinput));

      /**
       * **********************************************************************************************
       * Write Cleaned Data to BigQuery
       * **********************************************************************************************
       */
      transactionWithStoreLoc
          .apply(Convert.toRows())
          .apply(
              "StoreCorrectedTransactionDataToDW",
              BigQueryIO.<Row>write()
                  .useBeamSchema()
                  .withWriteDisposition(WriteDisposition.WRITE_APPEND)
                  //        .withTimePartitioning(new TimePartitioning().setField("timestamp"))
                  .to(
                      String.format(
                          "%s:%s",
                          options.getDataWarehouseOutputProject(),
                          options.getTransactionsBigQueryCleanTable())));

      return transactionWithStoreLoc.apply(
          Window.into(FixedWindows.of(Duration.standardSeconds(5))));
    }
  }

  public static class TransactionPerProductAndLocation
      extends PTransform<PCollection<TransactionEvent>, PCollection<StockAggregation>> {

    @Override
    public PCollection<StockAggregation> expand(PCollection<TransactionEvent> input) {

      PCollection<Row> aggregate =
          input
              .apply(Select.<TransactionEvent>fieldNames("product_id", "storeLocation.id"))
              .apply(
                  RenameFields.<Row>create()
                      .rename("id", "storeId")
                      .rename("product_id", "productId"));

      PCollection<Row> cnt =
          aggregate
              .apply(
                  Group.<Row>byFieldNames("productId", "storeId")
                      .aggregateField("storeId", Count.combineFn(), "count"))
              .apply(Select.fieldNames("key.storeId", "key.productId", "value.count"))
              .apply(
                  AddFields.<Row>create()
                      // Using DefaultValue as workaround for 2.19 bug with NPE with NULLABLE
                      .field("durationMS", FieldType.INT64, 1L)
                      .field("startTime", FieldType.INT64, 1L));

      return cnt.apply(Convert.to(StockAggregation.class));
    }
  }

  public static class CountGlobalStockFromTransaction
      extends PTransform<PCollection<StockAggregation>, PCollection<StockAggregation>> {

    @Override
    public PCollection<StockAggregation> expand(PCollection<StockAggregation> input) {
      return input
          .apply(Select.<StockAggregation>fieldNames("productId"))
          .apply(
              Group.<Row>byFieldNames("productId")
                  .aggregateField("productId", Count.combineFn(), "count"))
          .apply(Select.fieldNames("key.productId", "value.count"))
          .apply(
              AddFields.<Row>create()
                  // Need this field until we have @Nullable Schema check
                  .field("storeId", FieldType.INT32, 0)
                  .field("durationMS", FieldType.INT64, 1L)
                  .field("startTime", FieldType.INT64, 1L))
          .apply(Convert.to(StockAggregation.class))
          .apply(new CreateStockAggregatorMetadata(Duration.standardSeconds(5).getMillis()));
    }
  }

  public static class EnrichTransactionWithStoreLocation
      extends PTransform<PCollection<TransactionEvent>, PCollection<TransactionEvent>> {

    PCollectionView<Map<Integer, StoreLocation>> mapPCollectionView;

    public EnrichTransactionWithStoreLocation(
        PCollectionView<Map<Integer, StoreLocation>> mapPCollectionView) {
      this.mapPCollectionView = mapPCollectionView;
    }

    public EnrichTransactionWithStoreLocation(
        @Nullable String name, PCollectionView<Map<Integer, StoreLocation>> mapPCollectionView) {
      super(name);
      this.mapPCollectionView = mapPCollectionView;
    }

    public static EnrichTransactionWithStoreLocation create(
        PCollectionView<Map<Integer, StoreLocation>> mapPCollectionView) {
      return new EnrichTransactionWithStoreLocation(mapPCollectionView);
    }

    @Override
    public PCollection<TransactionEvent> expand(PCollection<TransactionEvent> input) {
      return input.apply(
          "AddStoreLocation",
          ParDo.of(
                  new DoFn<TransactionEvent, TransactionEvent>() {
                    @ProcessElement
                    public void process(
                        @Element TransactionEvent input,
                        @SideInput("mapPCollectionView") Map<Integer, StoreLocation> map,
                        OutputReceiver<TransactionEvent> o) {

                      if (map.get(input.storeId()) == null) {
                        throw new IllegalArgumentException(
                            String.format(" No Store found for id %s", input.storeId()));
                      }

                      o.output(
                          input.toBuilder().setStoreLocation(map.get(input.storeId())).build());
                    }
                  })
              .withSideInput("mapPCollectionView", mapPCollectionView));
    }
  }
}
