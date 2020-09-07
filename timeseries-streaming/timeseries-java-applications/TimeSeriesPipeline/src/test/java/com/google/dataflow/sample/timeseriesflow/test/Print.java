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

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.protobuf.util.Timestamps;
import java.util.stream.Collectors;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
/** This class is only intended for use during debug or demonstrations. */
public class Print<T> extends PTransform<PCollection<T>, PDone> {

  private static final Logger LOG = LoggerFactory.getLogger(Print.class);

  @Override
  @SuppressWarnings("rawtypes")
  public PDone expand(PCollection<T> input) {
    input.apply(
        ParDo.of(
            new DoFn<T, String>() {
              @ProcessElement
              public void process(@Element T input, BoundedWindow w) {
                LOG.info(input.getClass().toString());
                if (input instanceof Iterable) {
                  for (Object obj : (Iterable) input) {
                    print(obj);
                  }
                } else {
                  print(input);
                }
              }
            }));

    return PDone.in(input.getPipeline());
  }

  public static <T> void print(T input) {

    if (input instanceof TSAccumSequence) {
      LOG.info(printTSAccumSequence((TSAccumSequence) input));
    } else if (input instanceof TSAccum) LOG.info(printTSAccum((TSAccum) input));
    else {
      LOG.info(String.format("Print-%s", input.toString()));
    }
  }

  public static String printTSAccum(TSAccum tsAccum) {
    return String.join(
        ":",
        "AccumMetadata",
        tsAccum.getKey().getMajorKey(),
        tsAccum.getKey().getMinorKeyString(),
        Timestamps.toString(tsAccum.getLowerWindowBoundary()),
        Timestamps.toString(tsAccum.getUpperWindowBoundary()),
        "Metrics",
        tsAccum.getDataStoreMap().entrySet().stream()
            .map(x -> String.join(":", x.getKey(), x.getValue().toString().replace("\n", "")))
            .collect(Collectors.joining(":")));
  }

  public static String printTSAccumSequence(TSAccumSequence tsAccumSequence) {
    return String.join(
        ":",
        "AccumSequenceMetadata",
        tsAccumSequence.getKey().getMajorKey(),
        tsAccumSequence.getKey().getMinorKeyString(),
        Timestamps.toString(tsAccumSequence.getLowerWindowBoundary()),
        Timestamps.toString(tsAccumSequence.getUpperWindowBoundary()),
        "TSAccum",
        tsAccumSequence.getAccumsList().stream()
            .map(Print::printTSAccum)
            .collect(Collectors.joining()));
  }
}
