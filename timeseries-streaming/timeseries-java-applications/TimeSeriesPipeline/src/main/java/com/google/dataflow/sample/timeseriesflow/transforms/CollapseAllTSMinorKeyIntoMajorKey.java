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
package com.google.dataflow.sample.timeseriesflow.transforms;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

@Experimental
/**
 * Remove {@link TSKey#getMinorKeyString()} from Key, so that a Multi Variate time series can be
 * coalesced back into a single object.
 *
 * <p>Deprecated, use {@link GenerateMajorKeyWindowSnapshot#generateWindowSnapshot()}
 */
@Deprecated
public class CollapseAllTSMinorKeyIntoMajorKey
    extends PTransform<
        PCollection<KV<TSKey, TSAccumSequence>>,
        PCollection<KV<TSKey, Iterable<TSAccumSequence>>>> {

  @Override
  public PCollection<KV<TSKey, Iterable<TSAccumSequence>>> expand(
      PCollection<KV<TSKey, TSAccumSequence>> input) {

    return input
        .apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptor.of(TSKey.class), TypeDescriptor.of(TSAccumSequence.class)))
                .via(
                    x ->
                        KV.of(
                            TSKey.newBuilder(x.getKey()).clearMinorKeyString().build(),
                            TSAccumSequence.newBuilder(x.getValue()).setKey(x.getKey()).build())))
        .apply(GroupByKey.create());
  }
}
