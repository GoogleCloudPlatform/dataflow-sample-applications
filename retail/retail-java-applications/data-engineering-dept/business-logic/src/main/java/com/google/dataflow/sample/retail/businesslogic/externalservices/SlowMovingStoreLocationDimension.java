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

import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.dataflow.sample.retail.businesslogic.core.DeploymentAnnotations.NoPartialResultsOnDrain;
import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineClickStreamOptions;
import com.google.dataflow.sample.retail.businesslogic.core.options.RetailPipelineOptions;
import com.google.dataflow.sample.retail.businesslogic.core.utils.BigQueryUtil;
import com.google.dataflow.sample.retail.dataobjects.Dimensions.StoreLocation;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.MapCoder;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * You can retrieve side inputs from global windows to use them in a pipeline job with non-global
 * windows, like a FixedWindow. To slowly update global window side inputs in pipelines with
 * non-global windows:
 *
 * <p>Write a DoFn that periodically pulls data from a bounded source into a global window.
 *
 * <p>a. Use the GenerateSequence source transform to periodically emit a value.
 *
 * <p>b. Instantiate a data-driven trigger that activates on each element and pulls data from a
 * bounded source.
 *
 * <p>c. Fire the trigger to pass the data into the global window.
 *
 * <p>Create the side input for downstream transforms. The side input should fit into memory.
 *
 * <p>The global window side input triggers on processing time, so the main pipeline
 * nondeterministically matches the side input to elements in event time.
 *
 * <p>In this example a Location table is queried every {@link StoreLocations#refreshDuration} and
 * the data is processed into a {@link PCollectionView} for use by the pipeline.
 *
 * <p>The BigQuery client used here is based on a different version, than that used for BigQueryIO
 * in Beam SDK 2.19. The code in {@link StoreLocations#convertBQRowToStoreLocation(FieldValueList)}
 * can be removed once BigQueryIO is updated to use the newer client libraries.
 */
@NoPartialResultsOnDrain
public class SlowMovingStoreLocationDimension {

  public static class StoreLocations
      extends PTransform<PBegin, PCollectionView<Map<Integer, StoreLocation>>> {

    private static final Logger LOG = LoggerFactory.getLogger(StoreLocations.class);

    Duration refreshDuration;
    String tableRef;

    private StoreLocations(Duration refreshDuration, String tableRef) {
      this.refreshDuration = refreshDuration;
      this.tableRef = tableRef;
    }

    private StoreLocations(@Nullable String name, Duration refreshDuration, String tableRef) {
      super(name);
      this.refreshDuration = refreshDuration;
      this.tableRef = tableRef;
    }

    public static StoreLocations create(Duration updateDuration, String tableRef) {
      return new StoreLocations(updateDuration, tableRef);
    }

    @Override
    public PCollectionView<Map<Integer, StoreLocation>> expand(PBegin input) {

      Coder<Map<Integer, StoreLocation>> coder = null;
      SchemaRegistry schemaRegistry = input.getPipeline().getSchemaRegistry();
      TypeDescriptor<StoreLocation> type = TypeDescriptor.of(StoreLocation.class);

      try {
        coder =
            MapCoder.of(
                BigEndianIntegerCoder.of(),
                SchemaCoder.of(
                    schemaRegistry.getSchema(type),
                    type,
                    schemaRegistry.getToRowFunction(type),
                    schemaRegistry.getFromRowFunction(type)));
      } catch (NoSuchSchemaException e) {
        LOG.error("No Schema found for {} in SchemaRegistry", type);
      }

      if (input.getPipeline().getOptions().as(RetailPipelineOptions.class).getTestModeEnabled()) {
        Map<Integer, StoreLocation> map = new HashMap<>();
        map.put(
            1,
            StoreLocation.builder()
                .setId(1)
                .setLng(1D)
                .setLat(1D)
                .setState("CA")
                .setZip(90000)
                .setCity("City")
                .build());

        return input
            .apply(Create.<Map<Integer, StoreLocation>>of(map).withCoder(coder))
            .apply(View.asSingleton());
      }

      String project =
          input.getPipeline().getOptions().as(RetailPipelineClickStreamOptions.class).getProject();
      return input
          .apply("Impulse", GenerateSequence.from(0).withRate(1, refreshDuration))
          .apply(
              Window.<Long>into(new GlobalWindows())
                  .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                  .discardingFiredPanes())
          .apply(
              "Query store locations",
              ParDo.of(
                  new DoFn<Long, Map<Integer, StoreLocation>>() {

                    @ProcessElement
                    public void process(
                        @Element Long element, OutputReceiver<Map<Integer, StoreLocation>> o) {
                      Map<Integer, StoreLocation> map = new HashMap<>();

                      TableResult result =
                          BigQueryUtil.readDataFromBigQueryUsingQueryString(
                              project, storeLocationSQL(tableRef), "DataflowStoreLocation");

                      for (FieldValueList r : result.iterateAll()) {
                        StoreLocation sl = convertBQRowToStoreLocation(r);
                        map.put(sl.getId(), sl);
                      }

                      o.output(map);
                    }
                  }))
          .setCoder(coder)
          // View.asMap is not used,
          // see
          // https://stackoverflow.com/questions/54422510/how-to-solve-duplicate-values-exception-when-i-create-pcollectionviewmapstring#comment95717252_54422510
          .apply(View.asSingleton());
    }
  }

  private static String storeLocationSQL(String tableRef) {
    // TODO get lat & lng from table once the data supports it.
    return String.format("SELECT *, \"1\" as lat, \"1\" as lng FROM `%s`", tableRef);
  }

  private static StoreLocation convertBQRowToStoreLocation(FieldValueList fieldValues) {
    return StoreLocation.builder()
        .setId(Integer.valueOf((String) fieldValues.get("id").getValue()))
        .setZip(Integer.valueOf((String) fieldValues.get("zip").getValue()))
        .setCity(fieldValues.get("city").getStringValue())
        .setState(fieldValues.get("state").getStringValue())
        .setLat(fieldValues.get("lat").getDoubleValue())
        .setLng(fieldValues.get("lng").getDoubleValue())
        .build();
  }
}
