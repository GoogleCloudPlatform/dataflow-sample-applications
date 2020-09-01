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
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import java.math.BigDecimal;
import java.util.Iterator;
import org.apache.beam.sdk.transforms.DoFn;

/** Sum the gain and loss of a sequence of {@link TSAccumSequence} */
class ComputeSumWithCount extends DoFn<TSAccumSequence, TSAccum> {

  @ProcessElement
  public void process(ProcessContext pc) {

    Iterator<TSAccum> it = pc.element().getAccumsList().iterator();

    AccumCoreNumericBuilder current = new AccumCoreNumericBuilder(it.next());

    BigDecimal sum = TSDataUtils.getBigDecimalFromData(current.getSumOrNull());

    while (it.hasNext()) {
      AccumCoreNumericBuilder next = new AccumCoreNumericBuilder(it.next());
      BigDecimal nextLastValue = TSDataUtils.getBigDecimalFromData(next.getSumOrNull());

      sum = sum.add(nextLastValue);
    }

    AccumMABuilder builder =
        new AccumMABuilder(TSAccum.newBuilder().setKey(pc.element().getKey()).build());

    builder
        .setSum(CommonUtils.createNumData(sum.doubleValue()))
        .setMovementCount(CommonUtils.createNumData(pc.element().getAccumsCount()));

    pc.output(builder.build());
  }
}
