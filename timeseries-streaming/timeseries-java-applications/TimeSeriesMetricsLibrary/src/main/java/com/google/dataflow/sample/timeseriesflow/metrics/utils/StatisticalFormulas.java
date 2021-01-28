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
package com.google.dataflow.sample.timeseriesflow.metrics.utils;

import static java.math.BigDecimal.ROUND_HALF_UP;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;

public class StatisticalFormulas {

  public static BigDecimal computeExponentialMovingAverage(
      Iterator<TimeSeriesData.TSAccum> it, BigDecimal alpha) {

    BigDecimal currentWeightedSum = BigDecimal.valueOf(0);
    BigDecimal currentWeightedCount = BigDecimal.valueOf(0);
    BigDecimal previousWeightedSum = BigDecimal.valueOf(0);
    BigDecimal previousWeightedCount = BigDecimal.valueOf(0);
    BigDecimal ema;

    while (it.hasNext()) {
      AccumCoreNumericBuilder current = new AccumCoreNumericBuilder(it.next());
      BigDecimal currentLastValue = TSDataUtils.getBigDecimalFromData(current.getLastOrNull());
      // WeightedSum_n = value_n + (1 - alpha) * WeightedSum_n-1
      currentWeightedSum =
          currentLastValue.add(BigDecimal.valueOf(1).subtract(alpha).multiply(previousWeightedSum));

      // WeightedCount_n = 1 + (1 - alpha) * WeightedCount_n-1
      currentWeightedCount =
          BigDecimal.valueOf(1)
              .add(BigDecimal.valueOf(1).subtract(alpha).multiply(previousWeightedCount));

      previousWeightedSum = currentWeightedSum;
      previousWeightedCount = currentWeightedCount;
    }
    // EMA_n = WeightedSum_n / WeightedCount_n
    ema =
        (currentWeightedCount.compareTo(BigDecimal.ZERO) == 0)
            ? BigDecimal.ZERO
            : currentWeightedSum.divide(currentWeightedCount, RoundingMode.HALF_EVEN);
    return ema;
  }

  public static BigDecimal computeSimpleMovingAverage(Iterator<TimeSeriesData.TSAccum> it) {

    AccumCoreNumericBuilder current = new AccumCoreNumericBuilder(it.next());

    BigDecimal sum = TSDataUtils.getBigDecimalFromData(current.getSumOrNull());
    BigDecimal count = TSDataUtils.getBigDecimalFromData(current.getCountOrNull());

    while (it.hasNext()) {
      AccumCoreNumericBuilder next = new AccumCoreNumericBuilder(it.next());
      BigDecimal nextSumValue = TSDataUtils.getBigDecimalFromData(next.getSumOrNull());
      BigDecimal nextCount = TSDataUtils.getBigDecimalFromData(next.getCountOrNull());

      sum = sum.add(nextSumValue);
      count = count.add(nextCount);
    }

    return (count.compareTo(BigDecimal.ZERO) == 0)
        ? BigDecimal.ZERO
        : sum.divide(count, RoundingMode.HALF_EVEN);
  }

  private static final int SCALE = 10; // How many decimal points of precision to calculate stdDev

  /**
   * Private utility method to compute the square root of a BigDecimal in JDK 1.8
   *
   * @author barwnikk
   * @url https://stackoverflow.com/questions/13649703/square-root-of-bigdecimal-in-java
   * @url https://en.wikipedia.org/wiki/Methods_of_computing_square_roots#Babylonian_method
   */
  public static final BigDecimal TWO = BigDecimal.valueOf(2);

  public static BigDecimal sqrt(BigDecimal A, final int SCALE) {
    BigDecimal x0 = new BigDecimal("0");
    BigDecimal x1 = BigDecimal.valueOf(Math.sqrt(A.doubleValue()));

    while (!x0.equals(x1)) {
      x0 = x1;
      x1 =
          (x0.compareTo(BigDecimal.ZERO) == 0)
              ? BigDecimal.ZERO
              : A.divide(x0, SCALE, ROUND_HALF_UP);
      x1 = x1.add(x0);
      x1 =
          (x1.compareTo(BigDecimal.ZERO) == 0)
              ? BigDecimal.ZERO
              : x1.divide(TWO, SCALE, ROUND_HALF_UP);
    }
    return x1;
  }

  public static BigDecimal computeStandardDeviation(Iterator<TimeSeriesData.TSAccum> it) {

    AccumCoreNumericBuilder current = new AccumCoreNumericBuilder(it.next());

    BigDecimal sum =
        TSDataUtils.getBigDecimalFromData(current.getLastOrNull())
            .setScale(SCALE, RoundingMode.HALF_DOWN);
    BigDecimal sumPow =
        TSDataUtils.getBigDecimalFromData(current.getLastOrNull())
            .pow(2)
            .setScale(SCALE, RoundingMode.HALF_DOWN);
    BigDecimal count = BigDecimal.ONE;

    while (it.hasNext()) {
      AccumCoreNumericBuilder next = new AccumCoreNumericBuilder(it.next());
      BigDecimal nextSumValue = TSDataUtils.getBigDecimalFromData(next.getLastOrNull());
      BigDecimal nextSumPowValue = TSDataUtils.getBigDecimalFromData(next.getLastOrNull()).pow(2);
      BigDecimal nextCount = BigDecimal.ONE.setScale(SCALE, RoundingMode.HALF_DOWN);

      sum = sum.add(nextSumValue);
      sumPow = sumPow.add(nextSumPowValue);
      count = count.add(nextCount);
    }
    BigDecimal meanSquared =
        (count.compareTo(BigDecimal.ZERO) == 0)
            ? BigDecimal.ZERO
            : sumPow.divide(count, RoundingMode.HALF_EVEN);

    BigDecimal squaredMean =
        (count.compareTo(BigDecimal.ZERO) == 0)
            ? BigDecimal.ZERO
            : sum.divide(count, RoundingMode.HALF_EVEN).pow(2);

    BigDecimal stdDev;

    BigDecimal radicand = meanSquared.subtract(squaredMean);

    final boolean radicandZeroNegative =
        radicand.compareTo(BigDecimal.ZERO) == 0 || radicand.compareTo(BigDecimal.ZERO) < 0;
    stdDev = radicandZeroNegative ? BigDecimal.ZERO : sqrt(radicand, SCALE);

    return stdDev;
  }
}
