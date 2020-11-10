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
package com.google.dataflow.sample.timeseriesflow.examples.fsi.forex;

import com.google.dataflow.sample.timeseriesflow.TimeseriesStreamingOptions;
import org.apache.beam.sdk.annotations.Experimental;

@Experimental
public interface ExampleForexPipelineOptions extends TimeseriesStreamingOptions {

  // Option to specify BigQuery target table to push metrics
  String getBigQueryTableForTSAccumOutputLocation();

  void setBigQueryTableForTSAccumOutputLocation(String bigQueryTableForTSAccumOutputLocation);

  // Option to specify absolute path for input dataset
  String getInputPath();

  void setInputPath(String inputPath);

  String getTimezone();

  void setTimezone(String timezone);

  // Option to specify sampling period in seconds
  Integer getResampleSec();

  void setResampleSec(Integer resampleSec);

  // Option to specify rolling window to calculate metrics in seconds
  Integer getWindowSec();

  void setWindowSec(Integer resampleSec);
}
