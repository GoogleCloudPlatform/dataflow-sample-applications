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
import com.google.dataflow.sample.retail.dataobjects.Transaction.TransactionEvent;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

@Experimental
public class EnrichTransactionWithStoreLocation
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

                    if (map.get(input.getStoreId()) == null) {
                      throw new IllegalArgumentException(
                          String.format(" No Store found for id %s", input.getStoreId()));
                    }

                    o.output(
                        input.toBuilder().setStoreLocation(map.get(input.getStoreId())).build());
                  }
                })
            .withSideInput("mapPCollectionView", mapPCollectionView));
  }
}
