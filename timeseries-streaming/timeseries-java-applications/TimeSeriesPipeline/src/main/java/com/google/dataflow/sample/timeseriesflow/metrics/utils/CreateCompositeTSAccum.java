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
package com.google.dataflow.sample.timeseriesflow.metrics.utils;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.typeone.TSBaseCombiner;
import com.google.dataflow.sample.timeseriesflow.transforms.ConvertAccumToSequence;
import com.google.dataflow.sample.timeseriesflow.transforms.MergeTSAccum;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoValue
/**
 * Given a {@link PCollection<KV<TSKey,TSAccum>> } with different keys, create composite keys of the
 * values provided in KeyList.
 *
 * <p>The composite key will be based on lexical order, separated via hyphen. The computations will
 * be changed to accommodate multiple metrics for each sub key.
 *
 * <p>TSAccum of MajorKey = A and Minor Key = aa MajorKey = A and Minor Key = ab
 *
 * <p>Which have SUM as one of the computations, will become a TSAccum of
 *
 * <p>MajorKey = A and Minor Key = aa-ab With Metrics of aa-SUM ab-SUM
 *
 * <p>Composite Key TSAccum objects are used as a helper for creating for type 2 computations that
 * need output from from multiple type 1 outputs.
 */
@Experimental
public abstract class CreateCompositeTSAccum
    extends PTransform<PCollection<KV<TSKey, TSAccum>>, PCollection<KV<TSKey, TSAccumSequence>>> {

  private static final Logger LOG = LoggerFactory.getLogger(CreateCompositeTSAccum.class);

  public abstract List<TSKey> getKeysToCombineList();

  public abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_CreateCompositeTSAccum.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setKeysToCombineList(List<TSKey> keysToCombineList);

    public abstract CreateCompositeTSAccum build();
  }

  @Override
  public PCollection<KV<TSKey, TSAccumSequence>> expand(PCollection<KV<TSKey, TSAccum>> input) {

    // Generate composite TSKey value and key-metric values in the Data Store
    return input
        .apply("GenerateCompositeKey", ParDo.of(new GenerateCompositeKey(this)))
        .apply(GroupByKey.create())
        .apply(ParDo.of(new MergeTSAccum()))
        .apply(ConvertAccumToSequence.builder().build());
  }

  public static class GenerateCompositeKey extends DoFn<KV<TSKey, TSAccum>, KV<TSKey, TSAccum>> {

    CreateCompositeTSAccum createCompositeKey;
    final Map<String, String> compositeKey;

    public GenerateCompositeKey(CreateCompositeTSAccum convertAccumToSequence) {
      Preconditions.checkArgument(
          convertAccumToSequence.getKeysToCombineList().size() > 1,
          "Must provide two or more composite values.");
      compositeKey = keyList(convertAccumToSequence.getKeysToCombineList());
      this.createCompositeKey = convertAccumToSequence;
    }

    @ProcessElement
    public void process(@Element KV<TSKey, TSAccum> element, OutputReceiver<KV<TSKey, TSAccum>> o) {

      if (createCompositeKey.getKeysToCombineList().contains(element.getKey())) {

        Map<String, Data> map = new HashMap<>();
        element
            .getValue()
            .getDataStoreMap()
            .forEach(
                (key, value) ->
                    map.put(String.join("-", element.getKey().getMinorKeyString(), key), value));

        // We need to also pass in data to downstream to indicate if this TSAccum is all gapFilled
        // values
        int heartBeat = (element.getValue().getIsAllGapFillMessages()) ? 1 : 0;
        map.put(
            String.join("-", element.getKey().getMinorKeyString(), "hb"),
            Data.newBuilder().setIntVal(heartBeat).build());

        TSKey key =
            element
                .getKey()
                .toBuilder()
                .setMinorKeyString(compositeKey.get(element.getKey().getMajorKey()))
                .build();

        o.output(
            KV.of(
                key,
                element
                    .getValue()
                    .toBuilder()
                    .setKey(key)
                    .clearDataStore()
                    .putAllDataStore(map)
                    .putMetadata(TSBaseCombiner._BASE_COMBINER, "t")
                    .build()));
      }
    }
  }

  public static Map<String, String> keyList(List<TSKey> keys) {

    Map<String, List<String>> map = new HashMap<>();

    for (TSKey key : keys) {
      map.computeIfAbsent(key.getMajorKey(), x -> Lists.newArrayList())
          .add(key.getMinorKeyString());
    }

    Map<String, String> lookup = new HashMap<>();

    for (String key : map.keySet()) {
      List<String> values = map.get(key);
      values.sort(String::compareTo);
      lookup.put(key, String.join("-", values));
    }

    return lookup;
  }
}
