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
import org.apache.beam.vendor.grpc.v1p26p0.com.google.gson.Gson;

/**
 * Used as part of utility for creation of JSON with {@link Gson}. TODO Remove in favour of raw
 * String for the JSON.
 */
@DefaultCoder(AvroCoder.class)
public class TransactionsAVRO {
  public @Nullable long timestamp;
  public @org.apache.avro.reflect.Nullable int uid;
  public @org.apache.avro.reflect.Nullable String order_number;
  public @org.apache.avro.reflect.Nullable int user_id;
  public @org.apache.avro.reflect.Nullable int store_id;
  public @org.apache.avro.reflect.Nullable boolean returning;
  public @org.apache.avro.reflect.Nullable long time_of_sale;
  public @org.apache.avro.reflect.Nullable int department_id;
  public @org.apache.avro.reflect.Nullable int product_id;
  public @org.apache.avro.reflect.Nullable int product_count;
  public @org.apache.avro.reflect.Nullable float price;
  public @org.apache.avro.reflect.Nullable StoreLocationAvro storeLocation;

  /** Used as part of utility for creation of JSON with {@link Gson}. */
  @DefaultCoder(AvroCoder.class)
  public class StoreLocationAvro {
    public @Nullable long timestamp;
    public @org.apache.avro.reflect.Nullable int getId;
    public @org.apache.avro.reflect.Nullable int getZip;
    public @org.apache.avro.reflect.Nullable String getCity;
    public @org.apache.avro.reflect.Nullable String getState;
    public @org.apache.avro.reflect.Nullable Double getLat;
    public @org.apache.avro.reflect.Nullable Double getLng;
  }
}
