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
package com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.combiners.BTypeOne;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.datamap.AccumCoreNumericBuilder;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwo;
import com.google.dataflow.sample.timeseriesflow.metrics.BTypeTwoFn;
import com.google.dataflow.sample.timeseriesflow.metrics.CTypeTwo;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation;
import com.google.dataflow.sample.timeseriesflow.transforms.TypeTwoComputation.ComputeType;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The library requires TSDataPoints, which are a single data point, in order to correctly capture
 * Price * Vol at time t, the user is required to * price and vol together at before passing the
 * data to the library. For example if there is a market tick of AB{price : 2, volume : 10}, then
 * the following {@link com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint} can be
 * created:
 *
 * <p>{TSKey: {AB,price}, Data : 2}, {TSKey: {AB,volume}, Data : 10} , {TSKey: {AB,pricevolume},
 * Data : 20}
 *
 * <p>The user must then setmajorKeyName("AB"), setPriceByVolName("pricevolume") and
 * setVolName("volume")
 */
@TypeTwoComputation(computeType = ComputeType.COMPOSITE_KEY)
@Experimental
public class VWAPGFn extends CTypeTwo {

  private static final Logger LOG = LoggerFactory.getLogger(VWAPGFn.class);

  /** Options for {@link VWAPGFn} fn. */
  public interface VWAPOptions extends TSFlowOptions {

    List<String> getVWAPMajorKeyName();

    void setVWAPMajorKeyName(List<String> majorKeyName);

    String getVWAPPriceName();

    void setVWAPPriceName(String priceByQtyName);
  }

  PCollection<KV<TSKey, TSAccum>> vwap = null;

  public List<Class<? extends BTypeOne>> requiredTypeOne() {
    return ImmutableList.of(VWAPTypeOneComp.class);
  }

  @Override
  public PCollection<KV<TSKey, TSAccum>> expand(PCollection<KV<TSKey, TSAccumSequence>> input) {
    VWAPOptions vwapOptions = input.getPipeline().getOptions().as(VWAPOptions.class);

    String minorKeyNameForPrice = vwapOptions.getVWAPPriceName();
    List<String> majorKeyNames = vwapOptions.getVWAPMajorKeyName();

    // Construct all the keys that will have Vwap computed on them

    List<TSKey> majorKeys =
        majorKeyNames.stream()
            .map(
                x ->
                    TSKey.newBuilder()
                        .setMajorKey(x)
                        .setMinorKeyString(minorKeyNameForPrice)
                        .build())
            .collect(Collectors.toList());

    // Use the Vwap combiner to get the SUM(Price * Vol) and SUM(VOL) within the fixed window Type1.

    return input
        .apply(Filter.by(x -> majorKeys.contains(x.getKey())))
        .apply(
            BTypeTwo.builder()
                .setMapping(
                    new VWAPExtractOutPut().getContextualFn(input.getPipeline().getOptions()))
                .build());
  }

  public static class VWAPExtractOutPut extends BTypeTwoFn {

    @Override
    public SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>> getFunction(
        PipelineOptions options) {

      VWAPOptions vwapOptions = options.as(VWAPOptions.class);

      return (SerializableFunction<KV<TSKey, TSAccumSequence>, KV<TSKey, TSAccum>>)
          element -> {
            BigDecimal sumPowerVol = BigDecimal.ZERO;
            BigDecimal sumVol = BigDecimal.ZERO;

            AccumVWAPBuilder builder =
                new AccumVWAPBuilder(TSAccum.newBuilder().setKey(element.getKey()).build());

            for (TSAccum accum : element.getValue().getAccumsList()) {

              if (!accum.getIsAllGapFillMessages()) {
                AccumVWAPBuilder vwapBuilder = new AccumVWAPBuilder(accum);

                Data powerVol = vwapBuilder.getPriceVolOrNull();
                Data vol = vwapBuilder.getSumVolOrNull();

                Preconditions.checkNotNull(powerVol);
                Preconditions.checkNotNull(vol);

                sumPowerVol = sumPowerVol.add(TSDataUtils.getBigDecimalFromData(powerVol));
                sumVol = sumVol.add(TSDataUtils.getBigDecimalFromData(vol));
              }
            }

            builder.removePriceVol();
            builder.removeSumVol();

            if (sumPowerVol.equals(BigDecimal.ZERO) || sumVol.equals(BigDecimal.ZERO)) {
              builder.setVWAP(CommonUtils.createNumData(0D));
            } else {
              builder.setVWAP(
                  CommonUtils.createNumData(
                      sumPowerVol.divide(sumVol, BigDecimal.ROUND_DOWN).doubleValue()));
            }

            return KV.of(element.getKey(), builder.build());
          };
    }
  }

  @Override
  public List<String> excludeFromOutput() {
    return ImmutableList.of(AccumVWAPBuilder.PRICE_VOL, AccumVWAPBuilder.SUM_VOL);
  }

  public static class AccumVWAPBuilder extends AccumCoreNumericBuilder {

    public static final String VWAP = "VWAP";
    public static final String VOL = "Data::VOL";

    private static final String PRICE_VOL = "PRICE_VOL";
    private static final String SUM_VOL = "SUM_VOL";

    public AccumVWAPBuilder(TSAccum tsAccum) {
      super(tsAccum);
    }

    public void setPriceVol(Data data) {
      setValue(PRICE_VOL, data);
    }

    public Data getPriceVolOrNull() {
      return getValueOrNull(PRICE_VOL);
    }

    public void setSumVol(Data data) {
      setValue(SUM_VOL, data);
    }

    public Data getSumVolOrNull() {
      return getValueOrNull(SUM_VOL);
    }

    public void removePriceVol() {
      removeValue(PRICE_VOL);
    }

    public void removeSumVol() {
      removeValue(SUM_VOL);
      ;
    }

    public void setVWAP(Data data) {
      setValue(VWAP, data);
    }

    public Data getVWAPorNull() {
      return getValueOrNull(VWAP);
    }
  }
}
