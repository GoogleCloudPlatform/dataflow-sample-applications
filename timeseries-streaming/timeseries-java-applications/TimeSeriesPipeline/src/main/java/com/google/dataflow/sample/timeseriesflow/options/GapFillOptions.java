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
package com.google.dataflow.sample.timeseriesflow.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface GapFillOptions extends PipelineOptions {

  @Description("Is Gap filling enabled.")
  Boolean isGapFillEnabled();

  void setGapFillEnabled(Boolean ttlDurationSecs);

  @Description(
      "The time to live value for how long after a key does not output values we continue to gap fill.")
  Integer getTTLDurationSecs();

  void setTTLDurationSecs(Integer ttlDurationSecs);

  @Description(
      "The absolute time EpocMilli to stop gap filling, used in bootstrap pipelines when working with Bounded sources")
  Long getAbsoluteStopTimeMSTimestamp();

  void setAbsoluteStopTimeMSTimestamp(Long absoluteStopTimeMSTimestamp);

  @Description(
      "Enable hold and propagate of last known value for key, within the TTLDurationSec to gaps.")
  @Default.Boolean(false)
  Boolean getEnableHoldAndPropogateLastValue();

  void setEnableHoldAndPropogateLastValue(Boolean enableHoldAndPropogate);
}
