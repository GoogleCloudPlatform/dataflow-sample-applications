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

import com.google.dataflow.sample.retail.businesslogic.core.transforms.CreateStockAggregatorMetadata;
import com.google.dataflow.sample.retail.dataobjects.Stock.StockEvent;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Duration;

@Experimental
public class CountIncomingStockPerProductLocation
    extends PTransform<PCollection<StockEvent>, PCollection<StockAggregation>> {

  private Duration duration;

  public CountIncomingStockPerProductLocation(Duration duration) {
    this.duration = duration;
  }

  public CountIncomingStockPerProductLocation(@Nullable String name, Duration duration) {
    super(name);
    this.duration = duration;
  }

  @Override
  public PCollection<StockAggregation> expand(PCollection<StockEvent> input) {
    return input
        .apply("SelectProductIdStoreId", Select.<StockEvent>fieldNames("product_id", "store_id"))
        .apply(
            Group.<Row>byFieldNames("product_id", "store_id")
                .aggregateField("*", Count.combineFn(), "count"))
        .apply(
            "SelectProductIdStoreIdCount",
            Select.<Row>fieldNames("key.product_id", "key.store_id", "value.count"))
        .apply(
            AddFields.<Row>create()
                // Need this field until we have @Nullable Schema check
                .field("durationMS", FieldType.INT64)
                .field("startTime", FieldType.INT64))
        .apply(Convert.to(StockAggregation.class))
        .apply(new CreateStockAggregatorMetadata(duration.getMillis()));
  }
}
