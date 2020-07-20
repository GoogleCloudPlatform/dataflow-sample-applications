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
package com.google.dataflow.sample.retail.pipeine.test;

import com.google.dataflow.sample.retail.businesslogic.core.utils.test.avrotestobjects.ClickStreamEventAVRO;
import com.google.dataflow.sample.retail.businesslogic.core.utils.test.avrotestobjects.InventoryAVRO;
import com.google.dataflow.sample.retail.businesslogic.core.utils.test.avrotestobjects.TransactionsAVRO;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.GsonBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Small testing injector, to be used for integration testing. */
public class TestStreamGenerator extends PTransform<PBegin, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(TestStreamGenerator.class);

  public static final TupleTag<String> CLICKSTREAM = new TupleTag<String>() {};
  public static final TupleTag<String> TRANSACTION = new TupleTag<String>() {};
  public static final TupleTag<String> STOCK = new TupleTag<String>() {};

  @Override
  public PCollectionTuple expand(PBegin input) {

    return input
        .apply(GenerateSequence.from(0).to(3).withRate(1, Duration.standardSeconds(5)))
        .apply(
            ParDo.of(new CreateClickStream())
                .withOutputTags(CLICKSTREAM, TupleTagList.of(TRANSACTION).and(STOCK)));
  }

  private static class CreateClickStream extends DoFn<Long, String> {
    Gson gson = null;

    @Setup
    public void setUp() {
      gson = new GsonBuilder().serializeNulls().create();
    }

    @ProcessElement
    public void process(ProcessContext pc, @Timestamp Instant time) {

      /**
       * **********************************************************************************************
       * Generate a non purchase sequence
       * **********************************************************************************************
       */
      int pageReferrer = ThreadLocalRandom.current().nextInt(10);
      int currentPage = ThreadLocalRandom.current().nextInt(10);
      ;

      for (int i = 0; i < 3; i++) {

        ClickStreamEventAVRO click = new ClickStreamEventAVRO();
        click.timestamp = time.getMillis();
        click.pageRef = String.format("P%s", pageReferrer);
        click.pageTarget = String.format("P%s", currentPage);
        click.lng = 43.726075;
        click.lng = -71.642508;
        click.agent =
            "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";
        click.transaction = false;
        click.uid = 1L;
        click.event = "browse";

        pc.output(gson.toJson(click));
        LOG.debug(String.format("Genrating Msg: %s", gson.toJson(click)));

        pageReferrer = currentPage;
        currentPage = ThreadLocalRandom.current().nextInt(10);
      }

      ClickStreamEventAVRO click = new ClickStreamEventAVRO();
      click.timestamp = time.getMillis();
      click.pageRef = String.format("P%s", pageReferrer);
      click.pageTarget = String.format("P%s", currentPage);
      click.lng = 43.726075;
      click.lng = -71.642508;
      click.agent =
          "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";
      click.transaction = false;
      click.uid = 1L;
      click.event = "ERROR-CODE-B87769A";

      /**
       * **********************************************************************************************
       * Generate and error event.
       * **********************************************************************************************
       */
      pc.output(gson.toJson(click));
      LOG.debug(String.format("Genrating Msg: %s", gson.toJson(click)));

      /**
       * **********************************************************************************************
       * Generate missing uid event.
       * **********************************************************************************************
       */
      click.event = "browse";
      click.uid = null;
      click.sessionId = UUID.randomUUID().toString();
      pc.output(gson.toJson(click));
      LOG.debug(String.format("Genrating Msg: %s", gson.toJson(click)));

      /**
       * **********************************************************************************************
       * Generate missing lat lng events.
       * **********************************************************************************************
       */
      click.lat = null;
      click.lng = null;
      pc.output(gson.toJson(click));
      LOG.debug(String.format("Genrating Msg: %s", gson.toJson(click)));

      /**
       * **********************************************************************************************
       * Generate a non purchase sequence
       * **********************************************************************************************
       */
      pageReferrer = ThreadLocalRandom.current().nextInt(10);
      currentPage = ThreadLocalRandom.current().nextInt(10);

      List<String> clickstream = new ArrayList<>();
      Instant clickTime = time;

      for (int i = 0; i < 3; i++) {

        click = new ClickStreamEventAVRO();
        click.timestamp = clickTime.getMillis();
        click.pageRef = String.format("P%s", pageReferrer);
        click.pageTarget = String.format("P%s", currentPage);
        click.lng = 43.726075;
        click.lng = -71.642508;
        click.agent =
            "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";
        click.transaction = false;
        click.uid = 1L;
        click.event = "browse";

        pc.outputWithTimestamp(CLICKSTREAM, gson.toJson(click), clickTime);
        LOG.debug(String.format("Genrating Msg: %s", gson.toJson(click)));

        clickTime = clickTime.plus(Duration.standardSeconds(i + 2));
        pageReferrer = currentPage;
        currentPage = ThreadLocalRandom.current().nextInt(10);
      }

      clickTime = clickTime.plus(Duration.standardSeconds(1));

      ClickStreamEventAVRO addToCart = new ClickStreamEventAVRO();
      addToCart.timestamp = time.getMillis();
      addToCart.pageRef = String.format("P%s", pageReferrer);
      addToCart.pageTarget = String.format("P%s", currentPage);
      addToCart.lng = 43.726075;
      addToCart.lng = -71.642508;
      addToCart.agent =
          "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";
      addToCart.transaction = false;
      addToCart.uid = 1L;
      addToCart.event = "add-to-cart";

      clickstream.add(gson.toJson(addToCart));
      LOG.debug(String.format("Genrating Msg: %s", gson.toJson(addToCart)));

      clickTime = clickTime.plus(Duration.standardSeconds(1));

      ClickStreamEventAVRO purchase = new ClickStreamEventAVRO();
      purchase.timestamp = time.getMillis();
      purchase.pageRef = String.format("P%s", pageReferrer);
      purchase.pageTarget = String.format("P%s", currentPage);
      purchase.lng = 43.726075;
      purchase.lng = -71.642508;
      purchase.agent =
          "Mozilla/5.0 (iPhone; CPU iPhone OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148";
      purchase.transaction = false;
      purchase.uid = 1L;
      purchase.event = "purchase";

      clickstream.add(gson.toJson(purchase));
      LOG.debug(String.format("Genrating Msg: %s", gson.toJson(purchase)));

      clickTime = clickTime.plus(Duration.standardSeconds(1));

      TransactionsAVRO transaction = new TransactionsAVRO();

      transaction.department_id = 1;
      transaction.order_number = String.format("1-0075-%s", ThreadLocalRandom.current().nextLong());
      transaction.price = 1;
      transaction.product_count = 1;
      transaction.store_id = 1;
      transaction.time_of_sale = clickTime.getMillis();
      transaction.uid = 1;
      transaction.user_id = 1;
      transaction.timestamp = clickTime.getMillis();

      pc.outputWithTimestamp(TRANSACTION, gson.toJson(transaction), clickTime);
      LOG.debug(String.format("Genrating Msg: %s", gson.toJson(transaction)));
      clickTime = clickTime.plus(Duration.standardSeconds(10));
      InventoryAVRO stock = new InventoryAVRO();
      stock.count = 1;
      stock.store_id = 1;
      stock.product_id = 1;
      stock.timestamp = clickTime.getMillis();

      pc.outputWithTimestamp(STOCK, gson.toJson(stock), clickTime);
      LOG.debug(String.format("Genrating Msg: %s", gson.toJson(stock)));
    }
  }
}
