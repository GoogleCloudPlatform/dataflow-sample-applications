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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.validation;

import com.google.dataflow.sample.retail.businesslogic.externalservices.RetailCompanyServices;
import com.google.dataflow.sample.retail.dataobjects.ClickStream.Item;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

public class EventItemCorrectionService extends DoFn<Row, Row> {

  List<_WindowWrappedEvent> cache;
  RetailCompanyServices services;

  @Setup
  public void setup() {
    // Starting up super doper heavy service... ;-) well it is just a demo...
    services = new RetailCompanyServices();

    // setup our cache, in batch mode this
    cache = new ArrayList<>();
  }

  @ProcessElement
  public void process(
      @Element Row input, BoundedWindow w, @Timestamp Instant time, OutputReceiver<Row> o) {

    // Pass through if items are not needed by this event.

    if (!input.getArray("errors").contains(ValidateEventItems.CORRECTION_ITEM)) {
      o.output(input);
      return;
    }

    _WindowWrappedEvent packagedEvent = new _WindowWrappedEvent();
    packagedEvent.eventData = input;
    packagedEvent.eventWindow = w;
    packagedEvent.timestamp = time;

    cache.add(packagedEvent);
  }

  @FinishBundle
  public void finishBundle(FinishBundleContext fbc) {

    if (cache.size() > 0) {
      Map<String, Item> correctedEvents =
          services.convertItemIdsToFullText(
              populateIds(cache), cache.get(0).eventData.getRow("data").getSchema());

      // For each row, look up the value in the map and correct
      for (_WindowWrappedEvent event : cache) {

        Row row = event.eventData.getRow("data");
        Collection<Row> items = row.getArray("items");
        List<Row> updatedItems = new ArrayList<>();

        for (Row item : items) {
          String itemId = ((Row) item).getValue("item_id");
          Item correctedItem = correctedEvents.get(itemId);

          updatedItems.add(
              Row.fromRow(item)
                  .withFieldValue("item_name", correctedItem.getItemName())
                  .withFieldValue("item_category", correctedItem.getItemCat01())
                  .withFieldValue("item_brand", correctedItem.getItemBrand())
                  .build());
        }

        Row newDataRow = Row.fromRow(row).withFieldValue("items", updatedItems).build();

        fbc.output(
            Row.fromRow(event.eventData).withFieldValue("data", newDataRow).build(),
            event.timestamp,
            event.eventWindow);
      }
    }
    // Clear down the cache
    cache.clear();
  }

  private List<String> populateIds(List<_WindowWrappedEvent> events) {
    List<String> ids = new ArrayList<>();

    // Get a list of all ID's that we need information for.
    events.forEach(
        x ->
            x.eventData
                .getRow("data")
                .getArray("items")
                .forEach(y -> ids.add(((Row) y).getString("item_id"))));

    return ids;
  }

  /**
   * When using finish bundle we need information outside of just the element that we wish to
   * output. This is because Beam bundles can have different key / window per bundle.
   */
  private static class _WindowWrappedEvent {
    Row eventData;
    BoundedWindow eventWindow;
    Instant timestamp;
  }
}
