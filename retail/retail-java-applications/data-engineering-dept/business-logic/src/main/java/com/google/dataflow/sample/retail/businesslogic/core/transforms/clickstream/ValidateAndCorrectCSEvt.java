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
package com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream;

import com.google.common.collect.ImmutableList;
import com.google.dataflow.sample.retail.businesslogic.externalservices.RetailCompanyServices;
import com.google.dataflow.sample.retail.businesslogic.externalservices.RetailCompanyServices.LatLng;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;

/**
 * Clean clickstream:
 *
 * <p>Check if uid is null, check that there is a session-id, if both are missing send to dead
 * letter queue.
 *
 * <p>Check if lat / long are missing, if they are look up user in the user table.
 */
@Experimental
public class ValidateAndCorrectCSEvt extends PTransform<PCollection<Row>, PCollection<Row>> {

  private TupleTag<Row> main = new TupleTag<Row>() {};

  private TupleTag<Row> requiresCorrection = new TupleTag<Row>() {};

  private TupleTag<Row> deadLetter = new TupleTag<Row>() {};

  /**
   * When using finishbundle we need information outside of just the element that we wish to output.
   * This is because Beam bundles can have different key / window per bundle.
   */
  private static class _WindowWrappedEvent {
    Row eventData;
    BoundedWindow eventWindow;
    Instant timestamp;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {
    PCollectionTuple tuple =
        input.apply(
            "ValidateClickEvent",
            ParDo.of(new ValidateEvent(input.getSchema()))
                .withOutputTags(main, TupleTagList.of(ImmutableList.of(requiresCorrection))));

    // Fix missing UID

    PCollection<Row> fixedUID =
        tuple
            .get(requiresCorrection)
            .setRowSchema(input.getSchema())
            .apply(
                "AttachUIDBasedOnSession",
                ParDo.of(new AttachUIDBasedOnSessionIDUsingRetailService()))
            .setRowSchema(input.getSchema());

    // Fix Lat/Lng
    PCollection<Row> fixedLatLng =
        fixedUID
            .apply("AttachLatLng", ParDo.of(new AttachLatLongUsingRetailService()))
            .setRowSchema(input.getSchema());

    return PCollectionList.of(tuple.get(main).setRowSchema(input.getSchema()))
        .and(fixedLatLng)
        .apply(Flatten.pCollections());
  }

  /**
   * Will validate each event and out put
   *
   * <p>1 - Healthy events
   *
   * <p>2 - Events which have a missing UID
   *
   * <p>3 - Events which have a missing Lat/Long
   */
  public class ValidateEvent extends DoFn<Row, Row> {

    Schema schema = null;

    public ValidateEvent(Schema schema) {
      this.schema = schema;
    }

    @ProcessElement
    public void process(@Element Row input, MultiOutputReceiver o) {
      // Check if Uid is set and we have a sessionId
      // If there is missing UID and SessionID then nothing can be done to fix.
      if (input.getValue("user_id") == null && input.getValue("sessionId") != null) {
        o.get(requiresCorrection).output(input);
        return;
      }

      if (input.getValue("lat") == null || input.getValue("lng") == null) {
        o.get(requiresCorrection).output(input);
        return;
      }

      o.get(main).output(input);
    }
  }

  public class AttachUIDBasedOnSessionIDUsingRetailService extends DoFn<Row, Row> {

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

      // Pass through if UID ok.
      if (input.getValue("user_id") != null) {
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
      Map<String, Long> correctedEvents = services.convertSessionIdsToUids(populateIds(cache));
      for (_WindowWrappedEvent event : cache) {
        fbc.output(
            Row.fromRow(event.eventData)
                .withFieldValue(
                    "user_id", correctedEvents.get(event.eventData.getString("sessionId")))
                .build(),
            event.timestamp,
            event.eventWindow);
      }
      // Clear down the cache
      cache.clear();
    }
  }

  public class AttachLatLongUsingRetailService extends DoFn<Row, Row> {
    List<_WindowWrappedEvent> cache;
    RetailCompanyServices services;

    @Setup
    public void setup() {
      // Starting up super doper heavy service... ;-) well it is just a sample!...
      services = new RetailCompanyServices();
      // setup our cache, in batch mode this
      cache = new ArrayList<>();
    }

    @ProcessElement
    public void process(
        @Element Row input, BoundedWindow w, @Timestamp Instant time, OutputReceiver<Row> o) {

      // Bypass if element ok.
      if (input.getValue("lat") != null && input.getValue("lng") != null) {
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

      Map<String, LatLng> correctedEvents = services.convertMissingLatLongUids(populateIds(cache));
      for (_WindowWrappedEvent event : cache) {
        LatLng latLng = correctedEvents.get(event.eventData.getString("sessionId"));
        fbc.output(
            Row.fromRow(event.eventData)
                .withFieldValue("lat", latLng.lat)
                .withFieldValue("lng", latLng.lng)
                .build(),
            event.timestamp,
            event.eventWindow);
      }

      cache.clear();
    }
  }

  private List<String> populateIds(List<_WindowWrappedEvent> events) {
    List<String> ids = new ArrayList<>();
    events.forEach(x -> ids.add(x.eventData.getString("sessionId")));
    return ids;
  }
}
