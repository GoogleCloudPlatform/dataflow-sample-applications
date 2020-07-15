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
package com.google.dataflow.sample.retail.businesslogic.core.utils.test;

import com.google.dataflow.sample.retail.businesslogic.core.transforms.CreateStockAggregatorMetadata;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.WithKeys;
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

/** Unit tests for {@link CreateStockAggregatorMetadata}. */
@RunWith(JUnit4.class)
public class CreateStockAggregatorMetadataTest {
  private static final Long TIME = Instant.parse("2000-01-01T00:00:00").getMillis();

  private static final StockAggregation STOCK_AGGREGATION =
      StockAggregation.builder().setCount(1L).setProductId(1).setStoreId(1).build();

  @Rule public transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testMetadataAdded() {

    Duration duration = Duration.standardMinutes(1);

    PCollection<StockAggregation> result =
        pipeline
            .apply(
                Create.timestamped(
                    TimestampedValue.of(STOCK_AGGREGATION, Instant.ofEpochMilli(TIME))))

            // This big dance, is to emulate sending a combined element, to match expected timestamp
            // in the metadata extractor
            .apply(Window.into(FixedWindows.of(duration)))
            .apply(WithKeys.of(1))
            .apply(GroupByKey.create())
            .apply(Values.create())
            .apply(Flatten.iterables())
            .apply(new CreateStockAggregatorMetadata(duration.getMillis()));

    PAssert.that(result)
        .containsInAnyOrder(
            STOCK_AGGREGATION
                .toBuilder()
                .setStartTime(TIME)
                .setDurationMS(duration.getMillis())
                .build());
    pipeline.run();
  }
}
