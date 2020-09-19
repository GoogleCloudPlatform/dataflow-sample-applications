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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.StatisticalFormulas;
import java.math.BigDecimal;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

/** Sum the gain and loss of a sequence of {@link TSAccumSequence} */
class ComputeSimpleMovingAverageDoFn extends DoFn<TSAccumSequence, KV<TSKey, TSAccum>> {

  @ProcessElement
  public void process(ProcessContext pc, OutputReceiver<KV<TSKey, TSAccum>> o) {

    Iterator<TSAccum> it = pc.element().getAccumsList().iterator();

    BigDecimal avg = StatisticalFormulas.ComputeSimpleMovingAverage(it);

    AccumMABuilder maBuilder =
        new AccumMABuilder(TSAccum.newBuilder().setKey(pc.element().getKey()).build());

    maBuilder
        .setMovementCount(CommonUtils.createNumData(pc.element().getAccumsCount()))
        .setSimpleMovingAverage(CommonUtils.createNumData(avg.doubleValue()));

    o.output(KV.of(pc.element().getKey(), maBuilder.build()));
  }
}
