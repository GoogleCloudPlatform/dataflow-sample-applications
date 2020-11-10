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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.sdk.values.WindowingStrategy;

/**
 * Given a set of TSKey's, generate a PCollection of KV where all {@link TSKey#getMajorKey()} within
 * a window context are collapsed together. The Iterable, which is normally a {@link
 * TSAccumSequence} or {@link TSAccum} will be a 'snapshot' of all time series for that window.
 *
 * <p>This is done by: 1- Change the TSKey in the KV to have major key == the window context 2- GBK
 */
public class MajorKeyWindowSnapshot {

  public static <T> CollapseKeysToWindowContext<T> generateWindowSnapshot() {
    return new CollapseKeysToWindowContext<>();
  }

  private static class CollapseKeysToWindowContext<T>
      extends PTransform<PCollection<KV<TSKey, T>>, PCollection<Iterable<T>>> {

    @Override
    public PCollection<Iterable<T>> expand(PCollection<KV<TSKey, T>> input) {

      WindowingStrategy<?, ?> windowingStrategy = input.getWindowingStrategy();
      if (windowingStrategy.getWindowFn() instanceof GlobalWindows
          && windowingStrategy.getTrigger() instanceof DefaultTrigger
          && input.isBounded() != IsBounded.BOUNDED) {
        throw new IllegalStateException(
            "CollapseTSMinorKeys cannot be applied to non-bounded PCollection in the GlobalWindow without a trigger. Use a Window.into or Window.triggering transform prior to CollapseTSMinorKeys.");
      }

      return input
          .apply(new CollapseToWindowKey<>())
          .apply(GroupByKey.create())
          .apply(Values.create());
    }
  }

  /** Given a KV<TSKey,T> set the TSKey to window context. */
  private static class CollapseToWindowKey<T>
      extends PTransform<PCollection<KV<TSKey, T>>, PCollection<KV<TSKey, T>>> {

    @Override
    public PCollection<KV<TSKey, T>> expand(PCollection<KV<TSKey, T>> input) {
      return input.apply(Reify.windowsInValue()).apply(ParDo.of(new ReplaceKeyWithWindow<>()));
    }
  }

  private static class ReplaceKeyWithWindow<T>
      extends DoFn<KV<TSKey, ValueInSingleWindow<T>>, KV<TSKey, T>> {
    @ProcessElement
    public void process(
        @Element KV<TSKey, ValueInSingleWindow<T>> input, OutputReceiver<KV<TSKey, T>> o) {
      o.output(
          KV.of(
              TSKey.newBuilder().setMajorKey(input.getValue().getWindow().toString()).build(),
              input.getValue().getValue()));
    }
  }
}
