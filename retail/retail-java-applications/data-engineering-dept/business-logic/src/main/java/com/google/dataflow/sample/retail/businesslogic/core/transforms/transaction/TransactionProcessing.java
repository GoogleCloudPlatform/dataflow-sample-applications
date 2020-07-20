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
import com.google.dataflow.sample.retail.businesslogic.core.utils.JSONUtils;
import com.google.dataflow.sample.retail.businesslogic.core.utils.Print;
import com.google.dataflow.sample.retail.businesslogic.core.utils.WriteRawJSONMessagesToBigQuery;
import com.google.dataflow.sample.retail.businesslogic.externalservices.SlowMovingStoreLocationDimension.StoreLocations;
import com.google.dataflow.sample.retail.dataobjects.Dimensions.StoreLocation;
import com.google.dataflow.sample.retail.dataobjects.Transaction.TransactionEvent;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

@Experimental
public class TransactionProcessing
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
    if (options.getTestModeEnabled()) {
      transactionWithStoreLoc.apply(ParDo.of(new Print<>()));

    } else {
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
    }

    return transactionWithStoreLoc.apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));
  }
}
