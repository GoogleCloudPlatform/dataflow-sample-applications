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
package com.google.dataflow.sample.timeseriesflow;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

@Experimental
public interface TimeseriesStreamingOptions extends DataflowPipelineOptions {

  @Description("Fixed window length for type 1 computations in seconds")
  Integer getTypeOneComputationsLengthInSecs();

  void setTypeOneComputationsLengthInSecs(Integer typeOneComputationsLengthInSecs);

  @Description("Sliding window length for type 2 computations in seconds")
  Integer getTypeTwoComputationsLengthInSecs();

  void setTypeTwoComputationsLengthInSecs(Integer typeTwoComputationsLengthInSecs);

  @Description("Sliding window offset length for type 2 computations in seconds")
  Integer getTypeTwoComputationsOffsetLengthInSecs();

  void setTypeTwoComputationsOffsetLengthInSecs(
          Integer typeTwoComputationsOffsetLengthInSecs);

  @Description("Length of the Sequences, output after type 1 and type 2 computations are complete.")
  Integer getSequenceLengthInSeconds();

  @Description("Length of the Sequences, output after type 1 and type 2 computations are complete.")
  void setSequenceLengthInSeconds(Integer sequenceLengthInSeconds);

  @Description(
      "The time to live value for how long after a key does not output values we continue to gap fill.")
  Integer getTTLDurationSecs();

  @Description(
      "The time to live value for how long after a key does not output values we continue to gap fill.")
  void setTTLDurationSecs(Integer ttlDurationSecs);

  @Description(
      "The absolute time EpocMilli to stop gap filling, used in bootstrap pipelines when working with Bounded sources")
  Long getAbsoluteStopTimeMSTimestamp();

  @Description(
      "The absolute time EpocMilli to stop gap filling, used in bootstrap pipelines when working with Bounded sources")
  void setAbsoluteStopTimeMSTimestamp(Long absoluteStopTimeMSTimestamp);

  @Description(
      "Enable hold and propagate of last known value for key, within the TTLDurationSec to gaps.")
  @Default.Boolean(false)
  Boolean getEnableHoldAndPropogate();

  @Description(
      "Enable hold and propagate of last known value for key, within the TTLDurationSec to gaps.")
  void setEnableHoldAndPropogate(Boolean enableHoldAndPropogate);
}
