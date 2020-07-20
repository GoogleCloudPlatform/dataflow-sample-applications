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
package com.google.dataflow.sample.retail.pipeline;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.ClickstreamProcessing;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.WriteAggregationToBigQuery;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.CountGlobalStockUpdatePerProduct;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.CountIncomingStockPerProductLocation;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.StockProcessing;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.transaction.CountGlobalStockFromTransaction;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.transaction.TransactionPerProductAndLocation;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.transaction.TransactionProcessing;
import com.google.dataflow.sample.retail.businesslogic.core.utils.Print;
import com.google.dataflow.sample.retail.businesslogic.core.utils.ReadPubSubMsgPayLoadAsString;
import com.google.dataflow.sample.retail.dataobjects.Stock.StockEvent;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import com.google.dataflow.sample.retail.dataobjects.Transaction.TransactionEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;

/**
 * Primary pipeline using {@link ClickstreamProcessing}, {@link TransactionProcessing}, {@link
 * StockProcessing}.
 */
@Experimental
public class RetailDataProcessingPipeline {

  @VisibleForTesting public PCollection<String> testClickstreamEvents = null;

  @VisibleForTesting public PCollection<String> testTransactionEvents = null;

  @VisibleForTesting public PCollection<String> testStockEvents = null;

  public void startRetailPipeline(Pipeline p) throws Exception {

    RetailPipelineOptions options = p.getOptions().as(RetailPipelineOptions.class);

    boolean prodMode = !options.getTestModeEnabled();

    /**
     * **********************************************************************************************
     * Process Clickstream
     * **********************************************************************************************
     */
    PCollection<String> clickStreamJSONMessages = null;

    if (prodMode) {
      clickStreamJSONMessages =
          p.apply(
              "ReadClickStream",
              PubsubIO.readStrings()
                  .fromSubscription(options.getClickStreamPubSubSubscription())
                  .withTimestampAttribute("TIMESTAMP"));
    } else {
      checkNotNull(testClickstreamEvents, "In TestMode you must set testClickstreamEvents");
      clickStreamJSONMessages = testClickstreamEvents;
    }
    clickStreamJSONMessages.apply(new ClickstreamProcessing());

    /**
     * **********************************************************************************************
     * Process Transactions
     * **********************************************************************************************
     */
    PCollection<String> transactionsJSON = null;
    if (prodMode) {
      transactionsJSON =
          p.apply(
              "ReadTransactionStream",
              new ReadPubSubMsgPayLoadAsString(options.getTransactionsPubSubSubscription()));
    } else {
      checkNotNull(testTransactionEvents, "In TestMode you must set testClickstreamEvents");
      transactionsJSON = testTransactionEvents;
    }

    PCollection<TransactionEvent> transactionWithStoreLoc =
        transactionsJSON.apply(new TransactionProcessing());

    /**
     * **********************************************************************************************
     * Aggregate sales per item per location
     * **********************************************************************************************
     */
    PCollection<StockAggregation> transactionPerProductAndLocation =
        transactionWithStoreLoc.apply(new TransactionPerProductAndLocation());

    PCollection<StockAggregation> inventoryTransactionPerProduct =
        transactionPerProductAndLocation.apply(
            new CountGlobalStockFromTransaction(Duration.standardSeconds(5)));

    /**
     * **********************************************************************************************
     * Process Stock stream
     * **********************************************************************************************
     */
    PCollection<String> inventoryJSON = null;
    if (prodMode) {
      inventoryJSON =
          p.apply(
              "ReadStockStream",
              new ReadPubSubMsgPayLoadAsString(options.getInventoryPubSubSubscriptions()));
    } else {
      checkNotNull(testStockEvents, "In TestMode you must set testClickstreamEvents");
      inventoryJSON = testStockEvents;
    }

    PCollection<StockEvent> inventory = inventoryJSON.apply(new StockProcessing());

    /**
     * **********************************************************************************************
     * Aggregate Inventory delivery per item per location
     * **********************************************************************************************
     */
    PCollection<StockAggregation> incomingStockPerProductLocation =
        inventory.apply(new CountIncomingStockPerProductLocation(Duration.standardSeconds(5)));

    PCollection<StockAggregation> incomingStockPerProduct =
        incomingStockPerProductLocation.apply(
            new CountGlobalStockUpdatePerProduct(Duration.standardSeconds(5)));

    /**
     * **********************************************************************************************
     * Write Stock Aggregates - Combine Transaction / Inventory
     * **********************************************************************************************
     */
    PCollection<StockAggregation> inventoryLocationUpdates =
        PCollectionList.of(transactionPerProductAndLocation)
            .and(inventoryTransactionPerProduct)
            .apply(Flatten.pCollections());

    PCollection<StockAggregation> inventoryGlobalUpdates =
        PCollectionList.of(inventoryTransactionPerProduct)
            .and(incomingStockPerProduct)
            .apply(Flatten.pCollections());

    inventoryLocationUpdates.apply(
        WriteAggregationToBigQuery.create("StoreStockEvent", Duration.standardSeconds(10)));

    inventoryGlobalUpdates.apply(
        WriteAggregationToBigQuery.create("GlobalStockEvent", Duration.standardSeconds(10)));

    /**
     * **********************************************************************************************
     * Send Inventory updates to PubSub
     * **********************************************************************************************
     */
    PCollection<String> stockUpdates =
        inventoryGlobalUpdates.apply(
            "ConvertToPubSub", MapElements.into(TypeDescriptors.strings()).via(Object::toString));

    if (options.getTestModeEnabled()) {
      stockUpdates.apply(ParDo.of(new Print<>()));
    } else {
      stockUpdates.apply(PubsubIO.writeStrings().to(options.getAggregateStockPubSubOutputTopic()));
    }

    p.run();
  }

  public static void main(String[] args) throws Exception {

    RetailPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(RetailPipelineOptions.class);
    Pipeline p = Pipeline.create(options);

    new RetailDataProcessingPipeline().startRetailPipeline(p);
  }
}
