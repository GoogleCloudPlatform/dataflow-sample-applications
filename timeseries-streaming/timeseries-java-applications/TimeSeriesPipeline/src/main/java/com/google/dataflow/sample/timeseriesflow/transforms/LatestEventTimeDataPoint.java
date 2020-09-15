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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkState;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.protobuf.util.Timestamps;
import java.util.Iterator;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Instant;

/**
 * This is a modified version of the Apache Beam org.apache.beam.sdk.transforms.Latest transform.
 */
@Experimental
public class LatestEventTimeDataPoint {
  // Do not instantiate
  private LatestEventTimeDataPoint() {}

  /** Returns a {@link Combine.CombineFn} that selects the latest element among its inputs. */
  public static <T> Combine.CombineFn<TimestampedValue<T>, ?, T> combineFn() {
    return new LatestFn<>();
  }

  /**
   * Returns a {@link PTransform} that takes as input a {@code PCollection<KV<K, V>>} and returns a
   * {@code PCollection<KV<K, V>>} whose contents is the latest element per-key according to its
   * event time.
   */
  public static PTransform<PCollection<KV<TSKey, TSDataPoint>>, PCollection<KV<TSKey, TSDataPoint>>>
      perKey() {
    return new PerKey();
  }

  /**
   * A {@link Combine.CombineFn} that computes the latest element from a set of inputs.
   *
   * @param <T> Type of input element.
   * @see LatestEventTimeDataPoint
   */
  @VisibleForTesting
  static class LatestFn<T> extends Combine.CombineFn<TimestampedValue<T>, TimestampedValue<T>, T> {
    /** Construct a new {@link LatestFn} instance. */
    public LatestFn() {}

    @Override
    public TimestampedValue<T> createAccumulator() {
      return TimestampedValue.atMinimumTimestamp(null);
    }

    @Override
    public TimestampedValue<T> addInput(
        TimestampedValue<T> accumulator, TimestampedValue<T> input) {
      checkNotNull(accumulator, "accumulator must be non-null");
      checkNotNull(input, "input must be non-null");

      if (input.getTimestamp().isBefore(accumulator.getTimestamp())) {
        return accumulator;
      } else {
        return input;
      }
    }

    @Override
    public Coder<TimestampedValue<T>> getAccumulatorCoder(
        CoderRegistry registry, Coder<TimestampedValue<T>> inputCoder)
        throws CannotProvideCoderException {
      return NullableCoder.of(inputCoder);
    }

    @Override
    public Coder<T> getDefaultOutputCoder(
        CoderRegistry registry, Coder<TimestampedValue<T>> inputCoder)
        throws CannotProvideCoderException {
      checkState(
          inputCoder instanceof TimestampedValue.TimestampedValueCoder,
          "inputCoder must be a TimestampedValueCoder, but was %s",
          inputCoder);

      TimestampedValue.TimestampedValueCoder<T> inputTVCoder =
          (TimestampedValue.TimestampedValueCoder<T>) inputCoder;
      return NullableCoder.of(inputTVCoder.getValueCoder());
    }

    @Override
    public TimestampedValue<T> mergeAccumulators(Iterable<TimestampedValue<T>> accumulators) {
      checkNotNull(accumulators, "accumulators must be non-null");

      Iterator<TimestampedValue<T>> iter = accumulators.iterator();
      if (!iter.hasNext()) {
        return createAccumulator();
      }

      TimestampedValue<T> merged = iter.next();
      while (iter.hasNext()) {
        merged = addInput(merged, iter.next());
      }

      return merged;
    }

    @Override
    public T extractOutput(TimestampedValue<T> accumulator) {
      return accumulator.getValue();
    }
  }

  /** Implementation of {@link #perKey()}. */
  private static class PerKey
      extends PTransform<PCollection<KV<TSKey, TSDataPoint>>, PCollection<KV<TSKey, TSDataPoint>>> {

    @Override
    public PCollection<KV<TSKey, TSDataPoint>> expand(PCollection<KV<TSKey, TSDataPoint>> input) {
      checkNotNull(input);
      checkArgument(
          input.getCoder() instanceof KvCoder,
          "Input specifiedCoder must be an instance of KvCoder, but was %s",
          input.getCoder());

      return input
          .apply(
              MapElements.into(new TypeDescriptor<KV<TSKey, TimestampedValue<TSDataPoint>>>() {})
                  .<KV<TSKey, TSDataPoint>>via(
                      x ->
                          KV.of(
                              x.getKey(),
                              TimestampedValue.of(
                                  x.getValue(),
                                  Instant.ofEpochMilli(
                                      Timestamps.toMillis(x.getValue().getTimestamp()))))))
          .apply("Latest Value", Combine.perKey(new LatestFn<>()))
          .setCoder(KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class)));
    }
  }
}
