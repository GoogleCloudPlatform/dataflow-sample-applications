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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.common.CommonUtils;
import com.google.dataflow.sample.timeseriesflow.options.TFXOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class CommonUtilsTest {

  @Test(expected = IllegalArgumentException.class)
  public void testAddDataAllArgNotNull() {
    CommonUtils.sumNumericDataNullAsZero(null, null);
  }

  @Test()
  public void testAddDataWithLeftNull() {
    Data data = CommonUtils.sumNumericDataNullAsZero(null, CommonUtils.createNumData(1));
    Assert.assertEquals(data.getIntVal(), 1);
    data = CommonUtils.sumNumericDataNullAsZero(null, CommonUtils.createNumData(1L));
    Assert.assertEquals(data.getLongVal(), 1L);
    data = CommonUtils.sumNumericDataNullAsZero(null, CommonUtils.createNumData(1D));
    Assert.assertEquals(1D, data.getDoubleVal(), 0.0);
    data = CommonUtils.sumNumericDataNullAsZero(null, CommonUtils.createNumData(1F));
    Assert.assertEquals(1F, data.getFloatVal(), 0.0);
  }

  @Test()
  public void testAddDataWithRightNull() {
    Data data = CommonUtils.sumNumericDataNullAsZero(CommonUtils.createNumData(1), null);
    Assert.assertEquals(data.getIntVal(), 1);
    data = CommonUtils.sumNumericDataNullAsZero(CommonUtils.createNumData(1L), null);
    Assert.assertEquals(data.getLongVal(), 1L);
    data = CommonUtils.sumNumericDataNullAsZero(CommonUtils.createNumData(1D), null);
    Assert.assertEquals(1D, data.getDoubleVal(), 0.0);
    data = CommonUtils.sumNumericDataNullAsZero(CommonUtils.createNumData(1F), null);
    Assert.assertEquals(1F, data.getFloatVal(), 0.0);
  }

  @Test()
  public void testAddData() {
    Data data =
        CommonUtils.sumNumericDataNullAsZero(
            CommonUtils.createNumData(1), CommonUtils.createNumData(1));
    Assert.assertEquals(data.getIntVal(), 2);
    data =
        CommonUtils.sumNumericDataNullAsZero(
            CommonUtils.createNumData(1L), CommonUtils.createNumData(1L));
    Assert.assertEquals(data.getLongVal(), 2L);
    data =
        CommonUtils.sumNumericDataNullAsZero(
            CommonUtils.createNumData(1D), CommonUtils.createNumData(1D));
    Assert.assertEquals(2D, data.getDoubleVal(), 0.0);
    data =
        CommonUtils.sumNumericDataNullAsZero(
            CommonUtils.createNumData(1F), CommonUtils.createNumData(1F));
    Assert.assertEquals(2F, data.getFloatVal(), 0.0);
  }

  @Test()
  public void testComputeNumberOfSequenceTimesteps() {
    TFXOptions tfxOptions = PipelineOptionsFactory.create().as(TFXOptions.class);
    tfxOptions.setOutputTimestepLengthInSecs(10);
    tfxOptions.setTypeOneComputationsLengthInSecs(5);
    Assert.assertEquals(2, (int) CommonUtils.getNumOfSequenceTimesteps(tfxOptions));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testComputeNumberOfSequenceWithIndivisable() {
    TFXOptions tfxOptions = PipelineOptionsFactory.create().as(TFXOptions.class);
    tfxOptions.setOutputTimestepLengthInSecs(10);
    tfxOptions.setTypeOneComputationsLengthInSecs(6);
    Assert.assertEquals(2, (int) CommonUtils.getNumOfSequenceTimesteps(tfxOptions));
  }

  @Test(expected = NullPointerException.class)
  public void testComputeNumberOfSequenceWithNullSequence() {
    TFXOptions tfxOptions = PipelineOptionsFactory.create().as(TFXOptions.class);
    tfxOptions.setTypeOneComputationsLengthInSecs(6);
    Assert.assertEquals(2, (int) CommonUtils.getNumOfSequenceTimesteps(tfxOptions));
  }

  @Test(expected = NullPointerException.class)
  public void testComputeNumberOfSequenceWithNullFixedLength() {
    TFXOptions tfxOptions = PipelineOptionsFactory.create().as(TFXOptions.class);
    tfxOptions.setOutputTimestepLengthInSecs(10);
    Assert.assertEquals(2, (int) CommonUtils.getNumOfSequenceTimesteps(tfxOptions));
  }
}
