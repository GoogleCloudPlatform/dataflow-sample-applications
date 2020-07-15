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

import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.ClickstreamProcessing;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.WriteAggregationToBigQuery;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.Stock;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.Stock.CountGlobalStockFromLocationStock;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.Stock.CountIncomingStockPerProductLocation;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.transaction.CountGlobalStockFromTransaction;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.transaction.TransactionPerProductAndLocation;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.transaction.TransactionProcessing;
import com.google.dataflow.sample.retail.dataobjects.Stock.StockEvent;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import com.google.dataflow.sample.retail.dataobjects.Transaction.TransactionEvent;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

/**
 * Primary pipeline using {@link ClickstreamProcessing}, {@link TransactionProcessing}, {@link
 * Stock}.
 */
public class RetailDataProcessingPipeline {

  public static void main(String[] args) throws NoSuchSchemaException {

    RetailPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(RetailPipelineOptions.class);

    Pipeline p = Pipeline.create(options);

    /**
     * **********************************************************************************************
     * Process Clickstream
     * **********************************************************************************************
     */
    PCollection<String> clickStreamJSONMessages =
        p.apply(
            "ReadClickStream",
            PubsubIO.readStrings()
                .fromSubscription(options.getClickStreamPubSubSubscription())
                .withTimestampAttribute("TIMESTAMP"));

    clickStreamJSONMessages.apply(new ClickstreamProcessing());

    /**
     * **********************************************************************************************
     * Process Transactions
     * **********************************************************************************************
     */
    PCollection<TransactionEvent> transactionWithStoreLoc =
        TransactionProcessing.processTransactions(p);

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
    PCollection<StockEvent> inventory = Stock.processStockPipeline(p);

    /**
     * **********************************************************************************************
     * Aggregate Inventory delivery per item per location
     * **********************************************************************************************
     */
    PCollection<StockAggregation> incomingStockPerProductLocation =
        inventory.apply(new CountIncomingStockPerProductLocation());

    PCollection<StockAggregation> incomingStockPerProduct =
        incomingStockPerProductLocation.apply(new CountGlobalStockFromLocationStock());

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
    inventoryGlobalUpdates
        .apply("ConvertToPubSub", MapElements.into(TypeDescriptors.strings()).via(Object::toString))
        .apply(PubsubIO.writeStrings().to(options.getAggregateStockPubSubOutputTopic()));

    p.run();
  }
}
