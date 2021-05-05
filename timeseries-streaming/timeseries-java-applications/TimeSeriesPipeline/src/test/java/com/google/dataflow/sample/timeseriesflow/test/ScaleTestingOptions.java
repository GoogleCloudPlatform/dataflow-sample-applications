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
package com.google.dataflow.sample.timeseriesflow.test;

import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;

public interface ScaleTestingOptions extends TSFlowOptions {

  public Integer getNumSecs();

  public void setNumSecs(Integer typeOneComputationsLengthInSecs);

  public Integer getNumFeatures();

  public void setNumFeatures(Integer typeOneComputationsLengthInSecs);

  public Integer getNumKeys();

  public void setNumKeys(Integer numKeys);

  public Boolean getWithFileOutput();

  public void setWithFileOutput(Boolean withFileOutput);

  public Integer getPerfectRecNumberDataSecs();

  public void setPerfectRecNumberDataSecs(Integer endTimestamp);

  public Boolean getSkipEvens();

  public void setSkipEvens(Boolean skipEvens);
}
