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
package com.google.dataflow.sample.retail.businesslogic.core.utils.test.stock;

import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.CountGlobalStockUpdatePerProduct;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.stock.CountIncomingStockPerProductLocation;
import com.google.dataflow.sample.retail.dataobjects.Stock.StockEvent;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
/** Unit tests for {@link ClickstreamProcessing}. */
public class CountGlobalStockUpdatePerProductTest {

  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static final TimestampedValue<StockEvent> EVENT_PRODUCT_1_STORE_1 =
      TimestampedValue.of(
          StockEvent.builder().setProductId(1).setStoreId(1).setCount(1).build(),
          Instant.ofEpochMilli(TIME));

  private static final TimestampedValue<StockEvent> EVENT_PRODUCT_1_STORE_2 =
      TimestampedValue.of(
          StockEvent.builder().setProductId(1).setStoreId(2).setCount(1).build(),
          Instant.ofEpochMilli(TIME));

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testCountGroupStockByProduct() {

    Duration windowDuration = Duration.standardMinutes(5);

    PCollection<StockAggregation> countStockPerProductPerLocation =
        pipeline
            .apply(Create.timestamped(EVENT_PRODUCT_1_STORE_1, EVENT_PRODUCT_1_STORE_2))
            .apply(Window.into(FixedWindows.of(windowDuration)))
            .apply(new CountIncomingStockPerProductLocation(windowDuration))
            .apply(new CountGlobalStockUpdatePerProduct(windowDuration));

    StockAggregation stockAggregationStore =
        StockAggregation.builder()
            .setCount(2L)
            .setProductId(1)
            .setStartTime(TIME)
            .setDurationMS(windowDuration.getMillis())
            .build();

    PAssert.that(countStockPerProductPerLocation).containsInAnyOrder(stockAggregationStore);

    pipeline.run();
  }
}
