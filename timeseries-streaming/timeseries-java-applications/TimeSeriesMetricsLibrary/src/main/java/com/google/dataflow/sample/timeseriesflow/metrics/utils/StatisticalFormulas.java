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

  public static BigDecimal ComputeExponentialMovingAverage(
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
        (currentWeightedCount == BigDecimal.ZERO)
            ? BigDecimal.ZERO
            : currentWeightedSum.divide(currentWeightedCount, RoundingMode.HALF_EVEN);
    return ema;
  }

  public static BigDecimal ComputeSimpleMovingAverage(Iterator<TimeSeriesData.TSAccum> it) {

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

    BigDecimal avg =
        (count == BigDecimal.ZERO) ? BigDecimal.ZERO : sum.divide(count, RoundingMode.HALF_EVEN);
    return avg;
  }

  public static final String SQRT_METHOD = "BABYLONIAN"; // NEWTON
  public static final Integer SCALE =
      10; // How many decimal points of precision to calculate stdDev

  private static final BigDecimal SQRT_DIG = new BigDecimal(150);
  private static final BigDecimal SQRT_PRE = new BigDecimal(10).pow(SQRT_DIG.intValue());

  /**
   * Private utility method used to compute the square root of a BigDecimal in JDK 1.8.
   *
   * @author Luciano Culacciatti
   * @url http://www.codeproject.com/Tips/257031/Implementing-SqrtRoot-in-BigDecimal
   */
  private static BigDecimal sqrtNewtonRaphson(BigDecimal c, BigDecimal xn, BigDecimal precision) {
    BigDecimal fx = xn.pow(2).add(c.negate());
    BigDecimal fpx = xn.multiply(new BigDecimal(2));
    BigDecimal xn1 = fx.divide(fpx, 2 * SQRT_DIG.intValue(), RoundingMode.HALF_DOWN);
    xn1 = xn.add(xn1.negate());
    BigDecimal currentSquare = xn1.pow(2);
    BigDecimal currentPrecision = currentSquare.subtract(c);
    currentPrecision = currentPrecision.abs();
    if (currentPrecision.compareTo(precision) <= -1) {
      return xn1;
    }
    return sqrtNewtonRaphson(c, xn1, precision);
  }

  /**
   * Private utility method to compute the square root of a BigDecimal in JDK 1.8
   *
   * @author barwnikk
   * @url https://stackoverflow.com/questions/13649703/square-root-of-bigdecimal-in-java
   * @url https://en.wikipedia.org/wiki/Methods_of_computing_square_roots#Babylonian_method
   */
  private static final BigDecimal TWO = BigDecimal.valueOf(2);

  public static BigDecimal sqrt(BigDecimal A, final int SCALE) {
    BigDecimal x0 = new BigDecimal("0");
    BigDecimal x1 = new BigDecimal(Math.sqrt(A.doubleValue()));

    while (!x0.equals(x1)) {
      x0 = x1;
      x1 = A.divide(x0, SCALE, ROUND_HALF_UP);
      x1 = x1.add(x0);
      x1 = x1.divide(TWO, SCALE, ROUND_HALF_UP);
    }
    return x1;
  }

  public static BigDecimal ComputeStandardDeviation(Iterator<TimeSeriesData.TSAccum> it) {

    AccumCoreNumericBuilder current = new AccumCoreNumericBuilder(it.next());

    BigDecimal sum = TSDataUtils.getBigDecimalFromData(current.getLastOrNull()).setScale(SCALE, RoundingMode.HALF_DOWN);
    BigDecimal sumPow =
        TSDataUtils.getBigDecimalFromData(current.getLastOrNull()).pow(2).setScale(SCALE, RoundingMode.HALF_DOWN);
    // BigDecimal count = TSDataUtils.getBigDecimalFromData(current.getCountOrNull()).setScale(130);
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
    /**
     * We use the formula std_dev = sqrt(mean(x^2)-mean(x)^2) to obtain population standard
     * deviation (not sample) https://en.wikipedia.org/wiki/Standard_deviation
     */
    BigDecimal meanSquared =
        (count == BigDecimal.ZERO) ? BigDecimal.ZERO : sumPow.divide(count, RoundingMode.HALF_EVEN);

    BigDecimal squaredMean =
        (count == BigDecimal.ZERO)
            ? BigDecimal.ZERO
            : sum.divide(count, RoundingMode.HALF_EVEN).pow(2);

    BigDecimal stdDev;

    if (SQRT_METHOD == "NEWTON") {
      stdDev =
          (count == BigDecimal.ZERO)
              ? BigDecimal.ZERO
              : sqrtNewtonRaphson(
                  meanSquared.subtract(squaredMean),
                  new BigDecimal(1),
                  new BigDecimal(1).divide(SQRT_PRE));
    } else { // By default use Babylonian sqrt method, perf and precision are similar to Newton
      stdDev =
          (count == BigDecimal.ZERO)
              ? BigDecimal.ZERO
              : sqrt(meanSquared.subtract(squaredMean), SCALE);
    }

    return stdDev;
  }
}
