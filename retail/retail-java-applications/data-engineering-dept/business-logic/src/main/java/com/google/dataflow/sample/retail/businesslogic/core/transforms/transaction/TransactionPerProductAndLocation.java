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

import com.google.dataflow.sample.retail.dataobjects.Dimensions.StoreLocation;
import com.google.dataflow.sample.retail.dataobjects.StockAggregation;
import com.google.dataflow.sample.retail.dataobjects.Transaction.TransactionEvent;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.schemas.transforms.Group;
import org.apache.beam.sdk.schemas.transforms.Select;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class TransactionPerProductAndLocation
    extends PTransform<PCollection<TransactionEvent>, PCollection<StockAggregation>> {

  @Override
  public PCollection<StockAggregation> expand(PCollection<TransactionEvent> input) {

    input.getPipeline().getSchemaRegistry().registerPOJO(StoreLocation.class);

    PCollection<Row> aggregate =
        input.apply(
            "SelectProductStore", Select.<TransactionEvent>fieldNames("product_id", "store_id"));

    PCollection<Row> cnt =
        aggregate
            .apply(
                Group.<Row>byFieldNames("product_id", "store_id")
                    .aggregateField("store_id", Count.combineFn(), "count"))
            .apply(
                "SelectStoreProductCount",
                Select.fieldNames("key.store_id", "key.product_id", "value.count"))
            .apply(
                AddFields.<Row>create()
                    .field("durationMS", FieldType.INT64)
                    .field("startTime", FieldType.INT64));

    return cnt.apply(Convert.to(StockAggregation.class));
  }
}
