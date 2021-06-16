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

import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

@Experimental
/**
 * PipelineOptions to allow the Out of the box metrics to be called by name. For custom metrics use
 * {@link com.google.dataflow.sample.timeseriesflow.graph.GenerateComputations.Builder} .
 */
public interface GenerateComputationsOptions extends PipelineOptions {

  @Description("Type one computations, for example typeone.Sum")
  List<String> getTypeOneBasicMetrics();

  @Description("Type one computations, for example typeone.Sum")
  void setTypeOneBasicMetrics(List<String> value);

  @Description("Type two basic computations (order is preserved), for example typetwo.basic.BB")
  List<String> getTypeTwoBasicMetrics();

  @Description("Type two basic computations (order is preserved), for example typetwo.basic.BB")
  void setTypeTwoBasicMetrics(List<String> typeTwoBasicMetrics);

  @Description(
      "Type two basic computations (order is preserved), for example typetwo.complex.fsi.RSIGFn")
  List<String> getTypeTwoComplexMetrics();

  @Description(
      "Type two basic computations (order is preserved), for example typetwo.complex.fsi.RSIGFn")
  void setTypeTwoComplexMetrics(List<String> typeTwoBasicMetrics);
}
