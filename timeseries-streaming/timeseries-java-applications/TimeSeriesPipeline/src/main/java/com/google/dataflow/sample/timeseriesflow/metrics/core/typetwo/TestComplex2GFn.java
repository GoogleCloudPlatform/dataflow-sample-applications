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
package com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.CTypeTwo;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;

@VisibleForTesting
@Experimental
/** This is a dummy class used as a test artifact only. */
public class TestComplex2GFn extends CTypeTwo {

  PCollection<KV<TSKey, TSAccum>> results;

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
    // RSI requires Sum UP and Sum Down Type 2 computation.
    return input.apply(
        MapElements.into(CommonUtils.getKvTSAccumTypedescritors())
            .via(x -> KV.of(TSKey.newBuilder().build(), TSAccum.newBuilder().build())));
  }

  public interface TestComplexOptions extends TSFlowOptions {};

  public List<Class<? extends BTypeOne>> requiredTypeOne() {
    return null;
  }

  @Override
  public List<String> excludeFromOutput() {
    return null;
  }

  public static class AccumTestBuilder extends AccumCoreNumericBuilder {
    public AccumTestBuilder(TSAccum tsAccum) {
      super(tsAccum);
    }
  }
}
