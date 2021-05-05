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

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Description;

@Experimental
public interface TSFlowOptions
    extends DataflowPipelineOptions,
        GapFillOptions,
        GenerateComputationsOptions,
        TSOutputPipelineOptions {

  @Description("Fixed window length for type 1 computations in seconds")
  Integer getTypeOneComputationsLengthInSecs();

  void setTypeOneComputationsLengthInSecs(Integer value);

  @Description("Sliding window length for type 2 computations in seconds")
  Integer getTypeTwoComputationsLengthInSecs();

  void setTypeTwoComputationsLengthInSecs(Integer value);

  @Description("Sliding window offset length for type 2 computations in seconds")
  Integer getTypeTwoComputationsOffsetLengthInSecs();

  void setTypeTwoComputationsOffsetLengthInSecs(Integer value);

  @Description(
      "The number of timesteps that will be output is based on this value divided by the type 1 fixed window length.")
  Integer getOutputTimestepLengthInSecs();

  void setOutputTimestepLengthInSecs(Integer value);
}
