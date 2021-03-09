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

import com.google.auto.value.AutoValue;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation.ComputeType;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/**
 * Compute the Log Return from a {@link TSAccumSequence}. With a Sequence which has n values, only n
 * and n-1 will be used to compute the metric.
 */
@AutoValue
@Experimental
public abstract class LogRtn implements Serializable {

  public static Builder builder() {
    return new AutoValue_LogRtn.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract LogRtn build();
  }

  public LogRtnComputation create() {
    return new LogRtnComputation(this);
  }

  /** Compute log return from n and n-1 value */
  @TypeTwoComputation(computeType = ComputeType.SINGLE_KEY)
  public static class LogRtnComputation
      extends PTransform<PCollection<KV<TSKey, TSAccumSequence>>, PCollection<KV<TSKey, TSAccum>>> {

    LogRtn logRtn;

    public LogRtnComputation(LogRtn logRtn) {
      this.logRtn = logRtn;
    }

    public LogRtnComputation(@Nullable String name, LogRtn logRtn) {
      super(name);
      this.logRtn = logRtn;
    }

    @Override
    public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {

      return input.apply(ParDo.of(new LogRtnDoFn()));
    }
  }

  static class LogRtnDoFn extends DoFn<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> {

    @ProcessElement
    public void process(ProcessContext pc, OutputReceiver<KV<TSKey, TSAccum>> o) {

      Iterator<TSAccum> it = pc.element().getValue().getAccumsList().iterator();

      TSAccum last = it.next();
      TSAccum penultimate = null;

      while (it.hasNext()) {
        penultimate = last;
        last = it.next();
      }

      AccumLogRtnBuilder logRtnBuilder =
          new AccumLogRtnBuilder(TSAccum.newBuilder().setKey(pc.element().getKey()).build());

      if (penultimate == null) {
        o.output(
            KV.of(
                pc.element().getKey(),
                logRtnBuilder.setLogRtn(CommonUtils.createNumData(0D)).build()));
        return;
      }

      AccumCoreNumericBuilder lead = new AccumCoreNumericBuilder(last);
      AccumCoreNumericBuilder lag = new AccumCoreNumericBuilder(penultimate);

      BigDecimal bdLead = TSDataUtils.getBigDecimalFromData(lead.getLastOrNull());
      BigDecimal bdlag = TSDataUtils.getBigDecimalFromData(lag.getLastOrNull());

      if (!bdLead.equals(BigDecimal.ZERO) && !bdlag.equals(BigDecimal.ZERO)) {

        BigDecimal diff = bdLead.divide(bdlag, 9, RoundingMode.CEILING);

        // TODO use BigDecimal log
        Double result = Math.log(Math.abs(diff.doubleValue()));

        logRtnBuilder.setLogRtn(
            CommonUtils.createNumData((Double.isInfinite(result) || result.isNaN()) ? 0D : result));
      } else {

        logRtnBuilder.setLogRtn(CommonUtils.createNumData(0D));
      }
      o.output(KV.of(pc.element().getKey(), logRtnBuilder.build()));
    }
  }
}
