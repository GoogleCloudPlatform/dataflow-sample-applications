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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.protobuf.util.Timestamps;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Given a {@link KV<TSKey,ValueInSingleWindow>} push the window boundary into the TSAccum */
public class AddWindowBoundaryToTSAccum
    extends DoFn<KV<TSKey, ValueInSingleWindow<TSAccum>>, KV<TSKey, TSAccum>> {

  public static final Logger LOG = LoggerFactory.getLogger(AddWindowBoundaryToTSAccum.class);

  @ProcessElement
  public void process(
      @Element KV<TSKey, ValueInSingleWindow<TSAccum>> input,
      OutputReceiver<KV<TSKey, TSAccum>> o) {

    TSAccum.Builder tsAccum = input.getValue().getValue().toBuilder();

    if (input.getValue().getWindow() instanceof IntervalWindow) {
      IntervalWindow intervalWindow = (IntervalWindow) input.getValue().getWindow();
      tsAccum
          .setUpperWindowBoundary(Timestamps.fromMillis(intervalWindow.end().getMillis()))
          .setLowerWindowBoundary(Timestamps.fromMillis(intervalWindow.start().getMillis()));
    } else {
      LOG.error(" Bounded window detected instead of Interval Window, this is a bug!");
    }

    o.output(KV.of(input.getKey(), tsAccum.build()));
  }
}
