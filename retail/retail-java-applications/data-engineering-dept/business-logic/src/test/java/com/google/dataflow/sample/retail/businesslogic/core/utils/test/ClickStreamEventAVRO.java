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
package com.google.dataflow.sample.retail.businesslogic.core.utils.test;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

@DefaultCoder(AvroCoder.class)
public class ClickStreamEventAVRO {
  @Nullable long timestamp;
  @org.apache.avro.reflect.Nullable Long uid;
  @org.apache.avro.reflect.Nullable String sessionId;
  @org.apache.avro.reflect.Nullable String pageRef;
  @org.apache.avro.reflect.Nullable String pageTarget;
  @org.apache.avro.reflect.Nullable Double lat;
  @org.apache.avro.reflect.Nullable Double lng;
  @org.apache.avro.reflect.Nullable String agent;
  @org.apache.avro.reflect.Nullable String event;
  @org.apache.avro.reflect.Nullable boolean transaction;
}
