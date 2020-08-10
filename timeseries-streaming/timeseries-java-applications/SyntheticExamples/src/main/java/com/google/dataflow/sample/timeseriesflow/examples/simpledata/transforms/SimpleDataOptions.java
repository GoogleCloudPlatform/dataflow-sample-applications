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
package com.google.dataflow.sample.timeseriesflow.examples.simpledata.transforms;

import com.google.dataflow.sample.timeseriesflow.ExampleTimeseriesPipelineOptions;
import org.apache.beam.sdk.options.Description;

public interface SimpleDataOptions extends ExampleTimeseriesPipelineOptions {

  @Description(
      "Set the demo mode for the app. Current modes are 'example_1', 'example_2', 'example_3' , 'example_4")
  String getDemoMode();

  void setDemoMode(String value);

  @Description(
          "Enable sending outliers with the stream of synthetic data.")
  Boolean getWithOutliers();

  void setWithOutliers(Boolean value);
}
