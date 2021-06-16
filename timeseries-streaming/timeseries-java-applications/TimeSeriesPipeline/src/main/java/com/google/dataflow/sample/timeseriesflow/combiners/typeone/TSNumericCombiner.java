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
package com.google.dataflow.sample.timeseriesflow.combiners.typeone;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data.DataPointCase;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.combiners.TSCombiner;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
/** Base type 1 combiner, which computes First / Last / Min / Max / Count and Sum. */
public class TSNumericCombiner extends TSBaseCombiner implements TSCombiner {

  public static Logger LOG = LoggerFactory.getLogger(TSNumericCombiner.class);
  // Type one metrics list
  public final List<Class<? extends BTypeOne>> basicType1Metrics;

  public List<BTypeOne> fnCache;

  public TSNumericCombiner(List<Class<? extends BTypeOne>> basicType1Metrics) {
    Preconditions.checkNotNull(basicType1Metrics);

    this.basicType1Metrics = basicType1Metrics;

    fnCache = new ArrayList<>();
    for (Class<? extends BTypeOne> bTypeOne : basicType1Metrics) {
      try {
        fnCache.add(bTypeOne.newInstance());
      } catch (InstantiationException | IllegalAccessException e) {
        LOG.error("Unable to create instance of bTypeOne", e);
      }
    }
  }

  public static TSNumericCombiner combine(List<Class<? extends BTypeOne>> basicType1Metrics) {
    return new TSNumericCombiner(basicType1Metrics);
  }

  @Override
  public TSAccum mergeTypedDataAccum(TSAccum a, TSAccum b) {

    for (BTypeOne bTypeOne : fnCache) {
      a = bTypeOne.mergeDataAccums(a, b);
    }
    return a;
  }

  @Override
  public TSAccum addTypeSpecificInput(TSAccum accumulator, TSDataPoint dataPoint) {

    Preconditions.checkArgument(
        !dataPoint.getData().getDataPointCase().equals(DataPointCase.DATAPOINT_NOT_SET));

    for (BTypeOne bTypeOne : fnCache) {
      accumulator = bTypeOne.addInput(accumulator, dataPoint);
    }

    return accumulator;
  }
}
