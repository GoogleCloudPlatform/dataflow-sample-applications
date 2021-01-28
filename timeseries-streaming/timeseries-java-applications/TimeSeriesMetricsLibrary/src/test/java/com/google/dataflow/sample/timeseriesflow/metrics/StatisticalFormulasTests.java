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
package com.google.dataflow.sample.timeseriesflow.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.dataflow.sample.timeseriesflow.DerivedAggregations;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.metrics.utils.StatisticalFormulas;
import common.TSTestDataBaseline;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import org.junit.Test;

public class StatisticalFormulasTests {
  final TimeSeriesData.TSAccum NEGATIVE_ACCUM =
      TimeSeriesData.TSAccum.newBuilder()
          .setKey(TSTestDataBaseline.KEY_A_A)
          .setLowerWindowBoundary(TSTestDataBaseline.START_TIMESTAMP)
          .setUpperWindowBoundary(TSTestDataBaseline.PLUS_FIVE_SECS_TIMESTAMP)
          .putDataStore(DerivedAggregations.Indicators.LAST.name(), CommonUtils.createNumData(-1))
          .build();

  final TimeSeriesData.TSAccum ZERO_ACCUM =
      TimeSeriesData.TSAccum.newBuilder()
          .setKey(TSTestDataBaseline.KEY_A_A)
          .setLowerWindowBoundary(TSTestDataBaseline.START_TIMESTAMP)
          .setUpperWindowBoundary(TSTestDataBaseline.PLUS_FIVE_SECS_TIMESTAMP)
          .putDataStore(DerivedAggregations.Indicators.LAST.name(), CommonUtils.createNumData(0))
          .build();

  @Test
  public void constantNegativeDataInStdDev() {

    Iterator<TimeSeriesData.TSAccum> it =
        new ArrayList<>(Arrays.asList(NEGATIVE_ACCUM, NEGATIVE_ACCUM, NEGATIVE_ACCUM)).iterator();

    assertEquals(StatisticalFormulas.computeStandardDeviation(it), BigDecimal.ZERO);
  }

  @Test
  public void constantZeroDataInStdDev() {

    Iterator<TimeSeriesData.TSAccum> it =
        new ArrayList<>(Arrays.asList(ZERO_ACCUM, ZERO_ACCUM, ZERO_ACCUM)).iterator();

    assertEquals(StatisticalFormulas.computeStandardDeviation(it), BigDecimal.ZERO);
  }

  @Test
  public void sqrtOfNegativeShouldRaiseException() {
    final int SCALE = 10; // How many decimal points of precision to calculate stdDev
    final BigDecimal SQRT_DIG = new BigDecimal(150);
    final BigDecimal SQRT_PRE = new BigDecimal(10).pow(SQRT_DIG.intValue());

    /* Asserting Babylonian method */
    assertThrows(
        NumberFormatException.class,
        () -> {
          StatisticalFormulas.sqrt(BigDecimal.valueOf(-1), SCALE);
        });
  }
}
