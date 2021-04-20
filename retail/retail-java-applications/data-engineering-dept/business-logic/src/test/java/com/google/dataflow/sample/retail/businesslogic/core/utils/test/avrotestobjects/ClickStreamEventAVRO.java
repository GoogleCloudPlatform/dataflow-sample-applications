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
package com.google.dataflow.sample.retail.businesslogic.core.utils.test.avrotestobjects;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.annotations.SerializedName;

@DefaultCoder(AvroCoder.class)
/** Used as part of utility for creation of JSON with {@link Gson}. */
public class ClickStreamEventAVRO {
  public @Nullable long timestamp;

  @SerializedName(value = "user_id")
  public @Nullable Long uid;

  @SerializedName(value = "client_id")
  public @Nullable String clientId;

  @SerializedName(value = "event_datetime")
  public @Nullable String eventDateTime;

  public @Nullable String pageRef;
  public @Nullable String pageTarget;
  public @Nullable String agent;
  public @Nullable String event;
  public @Nullable boolean transaction;
}
