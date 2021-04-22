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

@DefaultCoder(AvroCoder.class)
/**
 * Used as part of utility for creation of JSON with {@link Gson}. TODO Remove in favour of raw
 * String for the JSON.
 */
public class InventoryAVRO {

  public @Nullable long timestamp;
  public @Nullable int count;
  public @Nullable int sku;
  public @Nullable int product_id;
  public @Nullable int store_id;
  public @Nullable int aisleId;
  public @Nullable String product_name;
  public @Nullable int departmentId;
  public @Nullable Float price;
  public @Nullable String recipeId;
  public @Nullable String image;
}
