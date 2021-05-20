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
package com.google.dataflow.sample.retail.pipeline.test;

import com.google.dataflow.sample.retail.businesslogic.core.utils.test.avrotestobjects.InventoryAVRO;
import com.google.dataflow.sample.retail.businesslogic.core.utils.test.avrotestobjects.TransactionsAVRO;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.ClickStreamEvent;
import com.google.dataflow.sample.retail.dataobjects.Ecommerce;
import com.google.dataflow.sample.retail.dataobjects.Item;
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
import org.apache.beam.vendor.grpc.v1p26p0.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.GsonBuilder;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Small testing injector, to be used for integration testing. */
public class TestStreamGenerator extends PTransform<PBegin, PCollectionTuple> {

  private static final Logger LOG = LoggerFactory.getLogger(TestStreamGenerator.class);

  public static final TupleTag<ClickStreamEvent> CLICKSTREAM = new TupleTag<ClickStreamEvent>() {};
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

  private static class CreateClickStream extends DoFn<Long, ClickStreamEvent> {
    Gson gson = null;

    DateTimeFormatter fm;

    @Setup
    public void setUp() {
      gson = new GsonBuilder().serializeNulls().create();
      fm = DateTimeFormat.forPattern("yyyy-MM-dd HH:MM:SS");
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

      for (int i = 0; i < 3; i++) {

        String sessionId = UUID.randomUUID().toString();

        ClickStreamEvent click =
            ClickStreamEvent.builder()
                .setClientId(sessionId)
                .setEventTime(time.toString(fm))
                .setClientId(UUID.randomUUID().toString())
                .setPage(String.format("P%s", pageReferrer))
                .setPagePrevious(String.format("P%s", currentPage))
                .setUid(1L)
                .setEvent("browse")
                .build();

        pc.output(click);
        LOG.debug(String.format("Generating Msg: %s", gson.toJson(click)));

        pageReferrer = currentPage;
        currentPage = ThreadLocalRandom.current().nextInt(10);
      }

      ClickStreamEvent.Builder click =
          ClickStreamEvent.builder()
              .setClientId(UUID.randomUUID().toString())
              .setEventTime(time.toString(fm))
              .setPage(String.format("P%s", pageReferrer))
              .setPagePrevious(String.format("P%s", currentPage))
              .setUid(1L)
              .setEvent("ERROR-CODE-B87769A");

      /**
       * **********************************************************************************************
       * Generate and error event.
       * **********************************************************************************************
       */
      pc.output(click.build());

      /**
       * **********************************************************************************************
       * Generate missing uid event.
       * **********************************************************************************************
       */
      click.setEvent("browse").setUid(null).setClientId(UUID.randomUUID().toString());
      pc.output(click.build());

      /**
       * **********************************************************************************************
       * Generate bad date format events.
       * **********************************************************************************************
       */
      click.setEventTime("BROKEN DATE FORMAT!!!").setClientId(UUID.randomUUID().toString());
      pc.output(click.build());

      /**
       * **********************************************************************************************
       * Generate events with date in the future
       * **********************************************************************************************
       */
      click
          .setEventTime(time.plus(Duration.standardDays(30)).toString(fm))
          .setClientId(UUID.randomUUID().toString());
      pc.output(click.build());

      /**
       * **********************************************************************************************
       * Generate a purchase sequence
       * **********************************************************************************************
       */
      pageReferrer = ThreadLocalRandom.current().nextInt(10);
      currentPage = ThreadLocalRandom.current().nextInt(10);

      String sessionId = UUID.randomUUID().toString();

      List<ClickStreamEvent> clickstream = new ArrayList<>();
      Instant clickTime = time;

      for (int i = 0; i < 3; i++) {

        click =
            ClickStreamEvent.builder()
                .setClientId(sessionId)
                .setEventTime(time.toString(fm))
                .setClientId(UUID.randomUUID().toString())
                .setPage(String.format("P%s", pageReferrer))
                .setPagePrevious(String.format("P%s", currentPage))
                .setUid(1L)
                .setEvent("browse");

        pc.outputWithTimestamp(CLICKSTREAM, click.build(), clickTime);

        clickTime = clickTime.plus(Duration.standardSeconds(i + 2));
        pageReferrer = currentPage;
        currentPage = ThreadLocalRandom.current().nextInt(10);
      }

      clickTime = clickTime.plus(Duration.standardSeconds(1));

      ClickStreamEvent.Builder addToCart = click.setEvent("add-to-cart");

      clickstream.add(addToCart.build());

      clickTime = clickTime.plus(Duration.standardSeconds(1));

      ClickStreamEvent.Builder purchase =
          click
              .setEvent("purchase")
              .setEcommerce(
                  Ecommerce.builder()
                      .setItems(
                          ImmutableList.of(
                              Item.builder()
                                  .setIndex(0)
                                  .setItemCat01("cat01")
                                  .setItemListId("1")
                                  .setPrice(1F)
                                  .setQuantity(1)
                                  .build()))
                      .build());

      clickstream.add(purchase.build());

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
      LOG.debug(String.format("Generating Msg: %s", gson.toJson(transaction)));
      clickTime = clickTime.plus(Duration.standardSeconds(10));
      InventoryAVRO stock = new InventoryAVRO();
      stock.count = 1;
      stock.store_id = 1;
      stock.product_id = 1;
      stock.timestamp = clickTime.getMillis();

      pc.outputWithTimestamp(STOCK, gson.toJson(stock), clickTime);
      LOG.debug(String.format("Generating Msg: %s", gson.toJson(stock)));

      /**
       * **********************************************************************************************
       * Generate a non-purchase single session across air-gap sequence
       * **********************************************************************************************
       */
      sessionId = UUID.randomUUID().toString();

      for (int i = 0; i < 3; i++) {

        click =
            ClickStreamEvent.builder()
                .setClientId(sessionId)
                .setEventTime(time.toString(fm))
                .setClientId(UUID.randomUUID().toString())
                .setPage(String.format("P%s", pageReferrer))
                .setPagePrevious(String.format("P%s", currentPage))
                .setUid(1L)
                .setEvent("browse");

        pc.outputWithTimestamp(CLICKSTREAM, click.build(), clickTime);

        clickTime = clickTime.plus(Duration.standardSeconds(i + 2));
        pageReferrer = currentPage;
        currentPage = ThreadLocalRandom.current().nextInt(10);
      }

      clickTime = clickTime.plus(Duration.standardMinutes(10));

      for (int i = 0; i < 3; i++) {

        click =
            ClickStreamEvent.builder()
                .setClientId(sessionId)
                .setEventTime(time.toString(fm))
                .setClientId(UUID.randomUUID().toString())
                .setPage(String.format("P%s", pageReferrer))
                .setPagePrevious(String.format("P%s", currentPage))
                .setUid(1L)
                .setEvent("browse");

        pc.outputWithTimestamp(CLICKSTREAM, click.build(), clickTime);
        LOG.debug(String.format("Generating Msg: %s", gson.toJson(click)));

        clickTime = clickTime.plus(Duration.standardSeconds(i + 2));
        pageReferrer = currentPage;
        currentPage = ThreadLocalRandom.current().nextInt(10);
      }
    }
  }
}
