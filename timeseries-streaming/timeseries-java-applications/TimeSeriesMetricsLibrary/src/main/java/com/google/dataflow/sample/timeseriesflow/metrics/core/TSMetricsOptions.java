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
package com.google.dataflow.sample.timeseriesflow.metrics.core;

import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.rsi.RSIGFn.RSIOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.fsi.vwap.VWAPGFn.VWAPOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.complex.rule.ValueInBoundsGFn.ValueInBoundsOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Max.MaxOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Min.MinOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typeone.Sum.SumOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.bb.BBFn.BBOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.logrtn.LogRtnFn.LogRtnOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.ma.MAFn.MAOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.stddev.StdDevFn.StdDevOptions;
import com.google.dataflow.sample.timeseriesflow.metrics.core.typetwo.basic.sumupdown.SumUpDownFn.SumUpDownOptions;
import org.apache.beam.sdk.annotations.Experimental;

/**
 * Metric options is not intended to directly hold options, it aggregates all options from all
 * Metric interfaces.
 */
@Experimental
public interface TSMetricsOptions
    extends MaxOptions,
        MinOptions,
        SumOptions,
        BBOptions,
        LogRtnOptions,
        MAOptions,
        StdDevOptions,
        SumUpDownOptions,
        RSIOptions,
        VWAPOptions,
        ValueInBoundsOptions {}
