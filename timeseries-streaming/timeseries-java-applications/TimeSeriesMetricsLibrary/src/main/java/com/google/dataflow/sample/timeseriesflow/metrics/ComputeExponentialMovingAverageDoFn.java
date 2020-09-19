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
package com.google.dataflow.sample.timeseriesflow.metrics;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.StatisticalFormulas;
import java.math.BigDecimal;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/**
 * This class calculates Exponential Moving Average by extending the Apache Beam DoFn class {@link
 * org.apache.beam.sdk.transforms.DoFn}, so it can be used in a pipeline as a PTransform, it
 * leverages the Google Time Series framework {@link com.google.dataflow.sample.timeseriesflow}
 *
 * <p>The constructor also accepts an input parameter called Alpha, which represents the degree of
 * weighting decrease, as a constant smoothing factor between 0 and 1, typically set as alpha = 2 /
 * (N + 1), where N is the number of time intervals.
 *
 * <p>The EMA is calculated "recursively" as documented in (<a
 * href="https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average">Wikipedia</a>
 * using the below formula:
 *
 * <p>EMA_n = WeightedSum_n / WeightedCount_n WeightedSum_n = value_n + (1 - alpha) *
 * WeightedSum_n-1 WeightedCount_n = 1 + (1 - alpha) * WeightedCount_n-1
 *
 * <p>This formula avoids introducing calculation errors when initializing the first estimate, and
 * is equivalent to using Pandas EMA with input parameter adjust=True and span=# of periods.
 */
public class ComputeExponentialMovingAverageDoFn extends DoFn<TSAccumSequence, KV<TSKey, TSAccum>> {
  private final BigDecimal alpha;

  public ComputeExponentialMovingAverageDoFn(BigDecimal alpha) {
    this.alpha = alpha;
  }

  @ProcessElement
  public void process(ProcessContext pc, OutputReceiver<KV<TSKey, TSAccum>> o) {
    Iterator<TimeSeriesData.TSAccum> it = pc.element().getAccumsList().iterator();
    AccumCoreNumericBuilder current;

    BigDecimal ema = StatisticalFormulas.ComputeExponentialMovingAverage(it, this.alpha);
    AccumMABuilder maBuilder =
        new AccumMABuilder(
            TimeSeriesData.TSAccum.newBuilder().setKey(pc.element().getKey()).build());
    maBuilder
        .setExponentialMovingAverage(CommonUtils.createNumData(ema.doubleValue()))
        .setMovementCount(CommonUtils.createNumData(pc.element().getAccumsCount()));

    o.output(KV.of(pc.element().getKey(), maBuilder.build()));
  }
}
