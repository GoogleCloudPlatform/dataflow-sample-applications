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
package com.google.dataflow.sample.timeseriesflow.common;

import com.google.common.base.Preconditions;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import java.math.BigDecimal;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Row.FieldValueBuilder;

@Experimental
/** Collection of utilities for working with Data points. */
public class TSDataUtils {

  /** Return a {@link BigDecimal} from a {@link Data} point. */
  public static BigDecimal getBigDecimalFromData(Data data) {

    switch (data.getDataPointCase()) {
      case DOUBLE_VAL:
        {
          return new BigDecimal(data.getDoubleVal());
        }
      case LONG_VAL:
        {
          return new BigDecimal(data.getLongVal());
        }
      case FLOAT_VAL:
        {
          return new BigDecimal(data.getFloatVal());
        }
      case INT_VAL:
        {
          return new BigDecimal(data.getIntVal());
        }
      case NUM_AS_STRING:
        {
          return new BigDecimal(data.getNumAsString());
        }
      default:
        throw new IllegalStateException(
            String.format("Data point must have a value but is %s", data.getDataPointCase()));
    }
  }

  /** Multiply two Data points, returning a BigDecimal */
  public static BigDecimal multiply(Data a, Data b) {
    return getBigDecimalFromData(a).multiply(getBigDecimalFromData(b));
  }

  /** Divide two Data points, returning a BigDecimal */
  public static BigDecimal divide(Data a, Data b) {
    // TODO allow user to pass in round method
    return getBigDecimalFromData(a).divide(getBigDecimalFromData(b), BigDecimal.ROUND_DOWN);
  }

  /** Add Data points, returning a BigDecimal */
  public static BigDecimal add(Data... data) {
    BigDecimal bigDecimal = new BigDecimal(0);
    for (Data value : data) {
      bigDecimal = bigDecimal.add(getBigDecimalFromData(value));
    }
    return bigDecimal;
  }

  /**
   * Returns {@link Data.DataPointCase} from either of the Data points that has case set. TODO
   * Determine if better way to solve for this usage.
   */
  public static Data.DataPointCase getCaseFromEitherDataValues(Data a, Data b) {

    // a or b may not have had the value set, so we need to return the first that has

    Data.DataPointCase dataPointCaseA = a.getDataPointCase();
    Data.DataPointCase dataPointCaseB = b.getDataPointCase();
    Data.DataPointCase dataPointCase;

    if (dataPointCaseA == Data.DataPointCase.DATAPOINT_NOT_SET
        && dataPointCaseB == Data.DataPointCase.DATAPOINT_NOT_SET) {
      dataPointCase = Data.DataPointCase.DATAPOINT_NOT_SET;
    } else if (dataPointCaseA != Data.DataPointCase.DATAPOINT_NOT_SET) {
      dataPointCase = dataPointCaseA;
    } else {
      dataPointCase = dataPointCaseB;
    }

    return dataPointCase;
  }

  /** Return the {@link Data} with the highest value, null is considered lower. */
  public static Data findMaxValue(Data a, Data b) {

    Preconditions.checkArgument(
        (CommonUtils.hasData(a) || CommonUtils.hasData(b)), "Both values can not be null.");

    // If A is not set then return B
    if (!CommonUtils.hasData(a)) {
      return b;
    }

    // If B is not set then return A
    if (!CommonUtils.hasData(b)) {
      return a;
    }

    switch (getCaseFromEitherDataValues(a, b)) {
      case DOUBLE_VAL:
        {
          return (a.getDoubleVal() > b.getDoubleVal()) ? a : b;
        }
      case LONG_VAL:
        {
          return (a.getLongVal() > b.getLongVal()) ? a : b;
        }
      case INT_VAL:
        {
          return (a.getIntVal() > b.getIntVal()) ? a : b;
        }
      case FLOAT_VAL:
        {
          return (a.getFloatVal() > b.getFloatVal()) ? a : b;
        }
      default:
        return a;
    }
  }

  /** Return minimum value, Null is not assumed to be min */
  public static Data findMinData(Data a, Data b) {

    if (a == null && b != null) {
      return b;
    }

    if (b == null && a != null) {
      return a;
    }

    // If neither A or B have had the value set then return A
    if (a.getDataPointCase() == Data.DataPointCase.DATAPOINT_NOT_SET
        && b.getDataPointCase() == Data.DataPointCase.DATAPOINT_NOT_SET) {
      return a;
    }

    // If A is not set then return B
    if (a.getDataPointCase() == Data.DataPointCase.DATAPOINT_NOT_SET) {
      return b;
    }

    // If B is not set then return A
    if (b.getDataPointCase() == Data.DataPointCase.DATAPOINT_NOT_SET) {
      return a;
    }

    switch (getCaseFromEitherDataValues(a, b)) {
      case INT_VAL:
        {
          return (a.getIntVal() < b.getIntVal()) ? a : b;
        }
      case DOUBLE_VAL:
        {
          return (a.getDoubleVal() < b.getDoubleVal()) ? a : b;
        }
      case LONG_VAL:
        {
          return (a.getLongVal() < b.getLongVal()) ? a : b;
        }
      case FLOAT_VAL:
        return (a.getFloatVal() < b.getFloatVal()) ? a : b;
      default:
        return a;
    }
  }

  /** Return a {@link Row} given a {@link Data} point and name. */
  public static Row generateValueRow(String metric, Data data) {

    FieldValueBuilder row = Row.withSchema(getDataRowSchema()).withFieldValue(METRIC_NAME, metric);

    switch (data.getDataPointCase()) {
      case CATEGORICAL_VAL:
        {
          return row.withFieldValue(STRING_ROW_NAME, data.getCategoricalVal()).build();
        }
      case NUM_AS_STRING:
        {
          return row.withFieldValue(NUM_AS_STRING_ROW_NAME, data.getNumAsString()).build();
        }
      case INT_VAL:
        {
          return row.withFieldValue(INTEGER_ROW_NAME, data.getIntVal()).build();
        }
      case FLOAT_VAL:
        {
          return row.withFieldValue(FLOAT_ROW_NAME, data.getFloatVal()).build();
        }
      case DOUBLE_VAL:
        {
          return row.withFieldValue(DOUBLE_ROW_NAME, data.getDoubleVal()).build();
        }
      case LONG_VAL:
        {
          return row.withFieldValue(LONG_ROW_NAME, data.getLongVal()).build();
        }
    }
    throw new IllegalArgumentException("Can not convert a Data point with no data!");
  }

  /** Return a zero value for the type of {@link Data} passed in. */
  public static Data getZeroValueForType(Data data) {
    switch (data.getDataPointCase()) {
      case CATEGORICAL_VAL:
        {
          return Data.newBuilder().setCategoricalVal("").build();
        }
      case NUM_AS_STRING:
        {
          return Data.newBuilder().setNumAsString("0").build();
        }
      case INT_VAL:
        {
          return Data.newBuilder().setIntVal(0).build();
        }
      case FLOAT_VAL:
        {
          return Data.newBuilder().setFloatVal(0F).build();
        }
      case DOUBLE_VAL:
        {
          return Data.newBuilder().setDoubleVal(0D).build();
        }
      case LONG_VAL:
        {
          return Data.newBuilder().setLongVal(0L).build();
        }
    }
    return Data.newBuilder().build();
  }

  /**
   * ********************************************************************** Schema for use in
   * outputing results to Row aware Sinks
   * **********************************************************************
   */
  public static final String STRING_ROW_NAME = "str_data";

  public static final String DOUBLE_ROW_NAME = "dbl_data";
  public static final String INTEGER_ROW_NAME = "int_data";
  public static final String FLOAT_ROW_NAME = "flt_data";
  public static final String LONG_ROW_NAME = "lng_data";
  public static final String NUM_AS_STRING_ROW_NAME = "num_as_str_data";
  public static final String METRIC_NAME = "metric";

  public static Schema getDataRowSchema() {
    return Schema.builder()
        .addStringField(METRIC_NAME)
        .addNullableField(STRING_ROW_NAME, FieldType.STRING)
        .addNullableField(DOUBLE_ROW_NAME, FieldType.DOUBLE)
        .addNullableField(INTEGER_ROW_NAME, FieldType.INT32)
        .addNullableField(FLOAT_ROW_NAME, FieldType.FLOAT)
        .addNullableField(LONG_ROW_NAME, FieldType.INT64)
        .build();
  }
}
