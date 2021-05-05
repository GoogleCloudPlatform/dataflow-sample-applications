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
package com.google.dataflow.sample.timeseriesflow.adaptors.fsi.data.cme;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
class LogElements<T> extends PTransform<PCollection<T>, PCollection<T>> {

  private static final Logger LOG = LoggerFactory.getLogger(LogElements.class);

  @Override
  public PCollection<T> expand(PCollection<T> input) {

    return input.apply(
        "Logging Elements",
        ParDo.of(
            new DoFn<T, T>() {

              @ProcessElement
              public void processElement(
                  @Element T element, OutputReceiver<T> out, BoundedWindow window) {

                String message = element.toString();

                if (!(window instanceof GlobalWindow)) {
                  message = message + " Window: " + window.toString();
                }

                LOG.info(message);

                out.output(element);
              }
            }));
  }
}
