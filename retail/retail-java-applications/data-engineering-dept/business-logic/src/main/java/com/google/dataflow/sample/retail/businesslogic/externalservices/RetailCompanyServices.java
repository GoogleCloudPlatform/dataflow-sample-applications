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
package com.google.dataflow.sample.retail.businesslogic.externalservices;

import com.google.dataflow.sample.retail.dataobjects.Item;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;

/**
 * This class represents a mock client to a microservice implemented by the Demo Retail company.
 *
 * <p>The class emulates communication between the Dataflow pipeline, and a hypothetical internal
 * microservice.
 *
 * <p>Real services will often take 10-100's of ms to respond, which cause back pressure within a
 * pipeline. This version of this mock does not cause push back.
 *
 * <p>TODO convert to a service which requires a few hundred ms to respond.
 */
@Experimental
public class RetailCompanyServices {

  public Map<String, Item> convertItemIdsToFullText(List<String> itemIds, Schema itemSchema) {

    Map<String, Item> map = new HashMap<>();

    Item item =
        Item.builder()
            .setItemBrand("item_brand")
            .setItemCat01("foo_category")
            .setItemName("foo_name")
            .build();

    itemIds.forEach(x -> map.put(x, item));
    return map;
  }
}
