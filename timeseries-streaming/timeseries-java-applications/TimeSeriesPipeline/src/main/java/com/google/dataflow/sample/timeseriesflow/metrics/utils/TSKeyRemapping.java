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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
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
 * Given a {@link PCollection<KV<TSKey,TSAccum>> } with different keys, create collections of values
 * provided in KeyList. The output is {@link MultiKeyTSAccum}
 *
 * <p>For each {@link List<TSKey>} provided a different {@link MultiKeyTSAccum} is generated which
 * contains all the elements requested.
 *
 * <p>This class is intended for usage with {@link
 * com.google.dataflow.sample.timeseriesflow.metrics.CTypeTwo} computations which need to bring
 * together two or more streams.
 */
@Experimental
public abstract class TSKeyRemapping<T>
    extends PTransform<PCollection<KV<TSKey, T>>, PCollection<KV<TSKey, Iterable<T>>>> {

  private static final Logger LOG = LoggerFactory.getLogger(TSKeyRemapping.class);

  public abstract HashMap<TSKey, Set<TSKey>> getKeyMap();

  HashMap<TSKey, Set<TSKey>> keyMap;

  public abstract Builder<T> toBuilder();

  public static <T> Builder<T> builder() {
    return new AutoValue_TSKeyRemapping.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder<T> {

    public abstract Builder<T> setKeyMap(HashMap<TSKey, Set<TSKey>> newKeyMap);

    public abstract TSKeyRemapping<T> build();
  }

  public TSKeyRemapping<T> withKeyRemappingOf(HashMap<TSKey, Set<String>> newKeyMap) {
    Preconditions.checkNotNull(newKeyMap);
    Preconditions.checkArgument(!newKeyMap.isEmpty(), "Key list can not be  empty!");

    return this;
  }

  @Override
  public PCollection<KV<TSKey, Iterable<T>>> expand(PCollection<KV<TSKey, T>> input) {

    // For each TSKeyGroup a hash value is created which is then used as a K in KV for grouping.
    // Once grouped all values in a group are added to a TSAccumMap.

    // Generate Map of TSKey->Hash

    keyMap = generateHashMap(getKeyMap());
    Set<TSKey> inlcudeKeys = new HashSet<>();
    getKeyMap().values().forEach(inlcudeKeys::addAll);

    // Generate composite TSKey value and key-metric values in the Data Store
    return input
        .apply(Filter.by(x -> inlcudeKeys.contains(x.getKey())))
        .apply(ParDo.of(new RemapKeys<T>(this)))
        .apply(GroupByKey.create());
  }

  /**
   * For every collection of keys generate a uuid. If a key appears n times its mapping set will
   * have n uuid's.
   */
  private static HashMap<TSKey, Set<TSKey>> generateHashMap(Map<TSKey, Set<TSKey>> mapping) {

    HashMap<TSKey, Set<TSKey>> hashMap = new HashMap<>();

    for (Entry<TSKey, Set<TSKey>> list : mapping.entrySet()) {
      // UUID which all of the keys in this list will map to
      for (TSKey key : list.getValue()) {
        hashMap.computeIfAbsent(key, x -> new HashSet<>()).add(list.getKey());
      }
    }
    return hashMap;
  }

  private static class RemapKeys<T> extends DoFn<KV<TSKey, T>, KV<TSKey, T>> {

    TSKeyRemapping<T> tsAccumToTSAccumMap;

    public RemapKeys(TSKeyRemapping<T> tsAccumToTSAccumMap) {
      this.tsAccumToTSAccumMap = tsAccumToTSAccumMap;
    }

    @ProcessElement
    public void process(@Element KV<TSKey, T> element, OutputReceiver<KV<TSKey, T>> o) {

      for (TSKey key : tsAccumToTSAccumMap.keyMap.get(element.getKey())) {
        o.output(KV.of(key, element.getValue()));
      }
    }
  }
}
