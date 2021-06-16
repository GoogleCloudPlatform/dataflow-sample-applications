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
package com.google.dataflow.sample.timeseriesflow.combiners;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import java.io.Serializable;

/** Allows for type one metrics to be added to the system. */
public abstract class BTypeOne implements Serializable {

  /** Define how a datapoints values should be added to a TSAccum for a type one computation */
  public abstract TSAccum addInput(TSAccum accumulator, TSDataPoint dataPoint);

  /** Define how two accums should be merged for the type one computation */
  public abstract TSAccum mergeDataAccums(TSAccum a, TSAccum b);
}
