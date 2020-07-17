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
package com.google.dataflow.sample.retail.pipeine.test;

import java.util.List;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class TestDataInjector {

  public class CreateClickStream extends PTransform<PBegin, PCollection<String>> {

    @Override
    public PCollection<String> expand(PBegin input) {
      return null;
    }
  }

  public static List<String> createClickstreamSession() {
    return null;
  }

  public static List<String> createBrokenClickstream() {
    return null;
  }

  public static List<String> createTransaction() {
    return null;
  }

  public static List<String> createStock() {
    return null;
  }
}
