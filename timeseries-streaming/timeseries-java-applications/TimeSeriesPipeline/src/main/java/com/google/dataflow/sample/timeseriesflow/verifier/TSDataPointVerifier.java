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
package com.google.dataflow.sample.timeseriesflow.verifier;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/** Ensure all TSDataPoint is valid. */
@Experimental
public class TSDataPointVerifier
    extends PTransform<PCollection<TSDataPoint>, PCollection<TSDataPoint>> {

  @Override
  public PCollection<TSDataPoint> expand(PCollection<TSDataPoint> input) {
    return input.apply(ParDo.of(new VerifyTSDataPoint()));
  }

  private static class VerifyTSDataPoint extends DoFn<TSDataPoint, TSDataPoint> {

    @ProcessElement
    public void process(@Element TSDataPoint input, OutputReceiver<TSDataPoint> o) {

      if (!(input.hasData() && CommonUtils.hasData(input.getData()))) {
        throw new IllegalStateException(
            String.format("TSDataPoint has no data. %s", input.toString()));
      }

      // Categorical values are not yet supported.
      if (!(input.getData().getCategoricalVal().isEmpty())) {
        throw new IllegalStateException(
            String.format("Categorical Values are not yet supported. %s", input.toString()));
      }

      if (!(input.hasKey())) {
        throw new IllegalStateException(
            String.format("TSDataPoint has no key. %s", input.toString()));
      }

      if (!(input.hasTimestamp())) {
        throw new IllegalStateException(
            String.format("TSDataPoint has no timestamp. %s", input.toString()));
      }

      o.output(input);
    }
  }
}
