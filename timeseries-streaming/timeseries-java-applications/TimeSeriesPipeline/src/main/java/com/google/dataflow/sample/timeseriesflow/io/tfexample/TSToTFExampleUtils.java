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
package com.google.dataflow.sample.timeseriesflow.io.tfexample;

import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.Data.DataPointCase;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccum;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSAccumSequence;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesTFExampleKeys.ExampleMetadata;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesTFExampleKeys.ExampleTypes;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tensorflow.example.BytesList;
import org.tensorflow.example.Example;
import org.tensorflow.example.Feature;
import org.tensorflow.example.Features;
import org.tensorflow.example.Features.Builder;
import org.tensorflow.example.FloatList;
import org.tensorflow.example.Int64List;

@Experimental
/** Utility to assist with conversion of TS objects to TF objects. */
public class TSToTFExampleUtils {

  private static final Logger LOG = LoggerFactory.getLogger(TSToTFExampleUtils.class);

  public static final TupleTag<Example> TIME_SERIES_FEATURE_METADATA = new TupleTag<Example>() {};
  public static final TupleTag<Example> TIME_SERIES_EXAMPLES = new TupleTag<Example>() {};

  public static FeaturesFromIterableAccumSequence createFeaturesFromIterableAccum(
      Integer timesteps) {
    return new FeaturesFromIterableAccumSequence(
        TIME_SERIES_FEATURE_METADATA, TIME_SERIES_EXAMPLES, timesteps);
  }

  public static Feature tfFeatureFromTSDataPoint(Data data) {
    Feature.Builder feature = Feature.newBuilder();

    switch (data.getDataPointCase()) {
      case DOUBLE_VAL:
        {
          feature.setFloatList(
              FloatList.newBuilder()
                  .addValue(BigDecimal.valueOf(data.getDoubleVal()).floatValue()));
          break;
        }
      case FLOAT_VAL:
        {
          feature.setFloatList(
              FloatList.newBuilder().addValue(BigDecimal.valueOf(data.getFloatVal()).floatValue()));
          break;
        }
      case LONG_VAL:
        {
          feature.setInt64List(Int64List.newBuilder().addValue(data.getLongVal()));
          break;
        }
      case INT_VAL:
        {
          feature.setInt64List(Int64List.newBuilder().addValue(data.getIntVal()));
          break;
        }
    }
    return feature.build();
  }

  /**
   * Converts a Iterable of {@LINK TSAccum} object into a Example proto with the following structure
   *
   * <p>Metadata
   *
   * <p>{@link ExampleMetadata#METADATA_SPAN_END_TS} Maximum timestamp of the span which the Example
   * covers
   *
   * <p>{@link ExampleMetadata#METADATA_SPAN_START_TS} Minimum timestamp of the span which the
   * Example covers
   *
   * <p>Features are added as MinorKey-Metric.
   *
   * <p>{ feature { key: "__CONFIG_TIMESTEPS-15" value { int64_list { value: 1 } } } feature { key:
   * "value-ABS_MOVING_AVERAGE" value { float_list { value: 61.369998931884766 .... .... } } } }
   *
   * <p>The special internal key __CONFIG_ is a temporary solution that allows configuration to be
   * sent to the TFX transform component. This will be replaced with the option that is coming to
   * the TFX library soon.
   */
  public static class FeaturesFromIterableAccumSequence
      extends PTransform<PCollection<Iterable<TSAccumSequence>>, PCollectionTuple> {

    public TupleTag<Example> names;
    public TupleTag<Example> examples;
    public Integer timesteps;

    public FeaturesFromIterableAccumSequence(
        TupleTag<Example> names, TupleTag<Example> examples, Integer timesteps) {
      this.names = names;
      this.examples = examples;
      this.timesteps = timesteps;
    }

    public FeaturesFromIterableAccumSequence(
        @Nullable String name,
        TupleTag<Example> names,
        TupleTag<Example> examples,
        Integer timesteps) {
      super(name);
      this.names = names;
      this.examples = examples;
      this.timesteps = timesteps;
    }

    @Override
    public PCollectionTuple expand(PCollection<Iterable<TSAccumSequence>> input) {

      return input.apply(
          ParDo.of(new ConvertAllKeyFeaturesToSingleExample(this))
              .withOutputTags(examples, TupleTagList.of(names)));
    }

    public static class ConvertAllKeyFeaturesToSingleExample
        extends DoFn<Iterable<TSAccumSequence>, Example> {

      List<WindowedValue<Example>> featureNames;

      FeaturesFromIterableAccumSequence featuresFromIterableAccumSequence;

      public ConvertAllKeyFeaturesToSingleExample(
          FeaturesFromIterableAccumSequence featuresFromIterableAccumSequence) {
        this.featuresFromIterableAccumSequence = featuresFromIterableAccumSequence;
      }

      @StartBundle
      public void startBundle() {
        featureNames = new ArrayList<>();
      }

      @FinishBundle
      public void finishBundle(FinishBundleContext c) {

        // TODO The Bounded Window object is odd
        featureNames.forEach(
            x ->
                c.output(
                    TSToTFExampleUtils.TIME_SERIES_FEATURE_METADATA,
                    x.getValue(),
                    x.getTimestamp(),
                    (BoundedWindow) x.getWindows().toArray()[0]));
      }

      @ProcessElement
      public void process(
          @Element Iterable<TSAccumSequence> accumsSequence,
          @Timestamp Instant timestamp,
          BoundedWindow window,
          ProcessContext pc,
          MultiOutputReceiver o) {

        try {
          Example.Builder example = Example.newBuilder();

          // Convert the data into a single TF.Example object
          example.setFeatures(
              convertAllKeyFeaturesToSingleExample(
                  accumsSequence, featuresFromIterableAccumSequence.timesteps));

          // Extract the feature names to output as metadata
          for (String key : example.getFeatures().getFeatureMap().keySet()) {
            featureNames.add(
                WindowedValue.<Example>of(
                    createMetadataFeature(
                        key, getTypeFromData(example.getFeatures().getFeatureMap().get(key))),
                    timestamp,
                    window,
                    pc.pane()));
          }

          o.get(featuresFromIterableAccumSequence.examples).output(example.build());

        } catch (UnsupportedEncodingException e) {
          LOG.error(e.getMessage());
        }
      }
    }

    /** Returns {@link Features} with order based on sequence time boundaries */
    public static Features convertAllKeyFeaturesToSingleExample(
        Iterable<TSAccumSequence> accums, Integer timesteps) throws UnsupportedEncodingException {

      Builder features = Features.newBuilder();

      Long upperBoundary = null;
      Long lowerBoundary = null;

      List<String> numericFeatureNames = new ArrayList<>();

      // We have multi array of TSAccumSequence, every Sequence is 'feature' as its a different
      // minor key Step We need to convert every TSAccumSequence to an array of [Feature[]]

      for (TSAccumSequence accumSequence : accums) {
        String featureName = accumSequence.getKey().getMinorKeyString();

        addNumericFeatures(featureName, features, accumSequence);
        numericFeatureNames.addAll(storeKey(featureName, features, accumSequence));

        // These values should be the same for all accums , if not there is an error
        upperBoundary =
            Optional.ofNullable(upperBoundary)
                .orElse(Timestamps.toMillis(accumSequence.getUpperWindowBoundary()));
        lowerBoundary =
            Optional.ofNullable(lowerBoundary)
                .orElse(Timestamps.toMillis(accumSequence.getLowerWindowBoundary()));
      }

      // Set these only once...
      features.putFeature(
          ExampleMetadata.METADATA_SPAN_START_TS.name(),
          Feature.newBuilder()
              .setInt64List(Int64List.newBuilder().addValue(lowerBoundary))
              .build());

      features.putFeature(
          ExampleMetadata.METADATA_SPAN_END_TS.name(),
          Feature.newBuilder()
              .setInt64List(Int64List.newBuilder().addValue(upperBoundary))
              .build());

      // This is a workaround as we are not able to use SequenceExample, this workaround will be
      // removed once SequenceExample is available within TFX.
      features.putFeature(
          String.format("__CONFIG_TIMESTEPS-%s", timesteps),
          Feature.newBuilder().setInt64List(Int64List.newBuilder().addValue(1L)).build());

      return features.build();
    }

    private static List<String> storeKey(
        String featureName, Builder features, TSAccumSequence accum) {
      List<String> numericFeatureNames = new ArrayList<>();

      // TODO this should not happen, get rid of this for better vallidation
      if (accum.getAccumsList().size() == 0) {
        return numericFeatureNames;
      }

      numericFeatureNames.add(String.format("%s-COUNT", featureName));

      // TODO BUG, if list changes midstream this will be wrong

      for (String s : accum.getAccumsList().get(0).getDataStoreMap().keySet()) {

        numericFeatureNames.add(String.format("%s-%s", featureName, s));
      }

      for (String s : accum.getSequenceDataMap().keySet()) {

        if (!accum
            .getSequenceDataMap()
            .get(s)
            .getDataPointCase()
            .equals(DataPointCase.CATEGORICAL_VAL)) {
          numericFeatureNames.add(String.format("%s-%s", featureName, s));
        }
      }

      return numericFeatureNames;
    }

    public static Builder addNumericFeatures(
        String featurePrefix, Builder features, TSAccumSequence accums) {
      // TODO when TS moves to be all Map<String,Data> based this will simplify these pieces

      // Each Accum has a custom Map
      Map<String, List<Data>> customFeatures = new HashMap<>();

      for (TSAccum tsAccum : accums.getAccumsList()) {
        // Loop through Map
        for (Entry<String, Data> map : tsAccum.getDataStoreMap().entrySet()) {
          // Append the values from this Accum into the feature Map
          customFeatures.putIfAbsent(map.getKey(), new ArrayList<>());
          customFeatures.get(map.getKey()).add(map.getValue());
        }
      }

      for (Entry<String, List<Data>> map : customFeatures.entrySet()) {
        features.putFeature(
            String.format("%s-%s", featurePrefix, map.getKey()),
            tfFeatureFromTSData(map.getValue()));
      }

      // Add all Span wide features
      for (String key : accums.getSequenceDataMap().keySet()) {
        features.putFeature(
            String.format("%s-%s", featurePrefix, key),
            tfFeatureFromTSDataPoint(accums.getSequenceDataMap().get(key)));
      }

      return features;
    }
  }

  public static Feature tfFeatureFromTSData(List<Data> data) {
    Feature.Builder feature = Feature.newBuilder();

    if (data.isEmpty()) {
      return feature.build();
    }

    switch (data.get(0).getDataPointCase()) {
      case DOUBLE_VAL:
        {
          FloatList.Builder list = FloatList.newBuilder();

          for (Data d : data) {
            if (d == null) {
              list.addValue(Float.NaN);
            } else {
              list.addValue(BigDecimal.valueOf(d.getDoubleVal()).floatValue());
            }
          }

          return feature.setFloatList(list).build();
        }
      case FLOAT_VAL:
        {
          FloatList.Builder list = FloatList.newBuilder();

          for (Data d : data) {
            if (d == null) {
              list.addValue(Float.NaN);
            } else {
              list.addValue(BigDecimal.valueOf(d.getFloatVal()).floatValue());
            }
          }
          return feature.setFloatList(list).build();
        }
      case LONG_VAL:
        {
          Int64List.Builder list = Int64List.newBuilder();

          for (Data d : data) {
            list.addValue(BigDecimal.valueOf(d.getLongVal()).longValue());
          }

          return feature.setInt64List(list).build();
        }
      case INT_VAL:
        {
          Int64List.Builder list = Int64List.newBuilder();

          for (Data d : data) {
            if (d == null) {
              list.addValue(0);
            } else {
              list.addValue(BigDecimal.valueOf(d.getIntVal()).intValue());
            }
          }

          return feature.setInt64List(list).build();
        }
      default:
        return feature.build();
    }
  }

  /**
   * Collapsing all org.apache.beam.sdk.extensions.timeseries.examples.timeseries into a single span
   * package. Expecting windowed input of {@KV<Long, @Example>>} which will be converted to a single
   * Example per span. The key will be the maximum timestamp of the Span. In batch mode this will
   */
  public static class CollapseMultipleStreamExampleIntoSingleExample
      extends PTransform<PCollection<KV<Long, Example>>, PCollectionTuple> {

    public TupleTag<Example> names;
    public TupleTag<Example> examples;

    public CollapseMultipleStreamExampleIntoSingleExample(
        TupleTag<Example> names, TupleTag<Example> examples) {
      this.names = names;
      this.examples = examples;
    }

    public CollapseMultipleStreamExampleIntoSingleExample(
        @Nullable String name, TupleTag<Example> names, TupleTag<Example> examples) {
      super(name);
      this.names = names;
      this.examples = examples;
    }

    @Override
    public PCollectionTuple expand(PCollection<KV<Long, Example>> input) {

      return input
          .apply(GroupByKey.create())
          .apply(
              ParDo.of(new CoalesceSpanExamples())
                  .withOutputTags(
                      TSToTFExampleUtils.TIME_SERIES_EXAMPLES,
                      TupleTagList.of(TSToTFExampleUtils.TIME_SERIES_FEATURE_METADATA)));
    }

    /**
     * Coalesce all time series spans into a single example with shape as per example below { TSKey
     * : { MAJOR_KEY : Max timestamp for span } START_SPAN_TS : Timestamp start Boundary from
     * Examples START_END_TS : Timestamp end Boundary from Examples ( will be the same as MAJOR_KEY
     * )
     *
     * <p>Features{ USDGBP-FIRST, USDGBP-LAST, USDGBP-..., USDEUR-FIRST, USDEUR-LAST, USDEUR-... } }
     * The names of all the features will also be output per span to a text file for use in
     * parse_feature in TFX.
     */
    private static class CoalesceSpanExamples extends DoFn<KV<Long, Iterable<Example>>, Example> {

      @ProcessElement
      public void process(@Element KV<Long, Iterable<Example>> examples, MultiOutputReceiver o)
          throws UnsupportedEncodingException {

        Builder builder = Features.newBuilder();

        List<Example> featureList = new ArrayList<>();

        try {

          Feature startSpan = null;
          Feature endSpan = null;

          for (Example e : examples.getValue()) {

            endSpan =
                Optional.ofNullable(startSpan)
                    .orElse(
                        e.getFeatures()
                            .getFeatureMap()
                            .get(ExampleMetadata.METADATA_SPAN_END_TS.name()));
            startSpan =
                Optional.ofNullable(startSpan)
                    .orElse(
                        e.getFeatures()
                            .getFeatureMap()
                            .get(ExampleMetadata.METADATA_SPAN_START_TS.name()));

            // Get all non-metadata features
            BytesList.Builder nonMetadataFeatures = BytesList.newBuilder();

            List<String> nonMetadataKeys =
                e.getFeatures().getFeatureMap().keySet().stream()
                    .filter(x -> !x.startsWith("METADATA_"))
                    .collect(Collectors.toList());
            nonMetadataKeys.forEach(x -> nonMetadataFeatures.addValue(ByteString.copyFromUtf8(x)));

            // There will always be a MajorKey at pos 0
            String majorKey =
                e.getFeatures()
                    .getFeatureMap()
                    .get(ExampleMetadata.METADATA_MAJOR_KEY.name())
                    .getBytesList()
                    .getValue(0)
                    .toString("UTF-8");

            // Place all features into Example
            for (ByteString f : nonMetadataFeatures.getValueList()) {
              String name = String.join("-", majorKey, f.toString("UTF-8"));
              builder.putFeature(name, e.getFeatures().getFeatureMap().get(f.toString("UTF-8")));
            }
          }

          builder.putFeature(
              ExampleMetadata.METADATA_MAJOR_KEY.name(),
              Feature.newBuilder()
                  .setBytesList(
                      BytesList.newBuilder()
                          .addValue(ByteString.copyFromUtf8(String.valueOf(examples.getKey()))))
                  .build());

          builder.putFeature(ExampleMetadata.METADATA_SPAN_START_TS.name(), startSpan);

          builder.putFeature(ExampleMetadata.METADATA_SPAN_END_TS.name(), endSpan);

          List<Example> featureNameType = new ArrayList<>();

          for (String key : builder.getFeatureMap().keySet()) {

            featureNameType.add(
                createMetadataFeature(key, getTypeFromData(builder.getFeatureMap().get(key))));
          }

          featureNameType.forEach(
              x -> o.get(TSToTFExampleUtils.TIME_SERIES_FEATURE_METADATA).output(x));

          o.get(TSToTFExampleUtils.TIME_SERIES_EXAMPLES)
              .output(Example.newBuilder().setFeatures(builder).build());

        } catch (UnsupportedEncodingException ex) {
          ex.printStackTrace();
        }
      }
    }
  }

  /** Return the type for a given data type, to be stored in Metadatafile. */
  public static ExampleTypes getTypeFromData(Feature feature) {

    switch (feature.getKindCase()) {
      case BYTES_LIST:
        return ExampleTypes.BYTE;
      case INT64_LIST:
        return ExampleTypes.INT64;
      case FLOAT_LIST:
        return ExampleTypes.FLOAT;
      default:
        throw new IllegalArgumentException("Feature can not have no value");
    }
  }

  /** Given a Metadata Name and a Type return an Example */
  public static Example createMetadataFeature(String name, ExampleTypes type) {
    return Example.newBuilder()
        .setFeatures(
            Features.newBuilder()
                .putFeature(
                    "NAME",
                    Feature.newBuilder()
                        .setBytesList(
                            BytesList.newBuilder().addValue(ByteString.copyFromUtf8(name)))
                        .build())
                .putFeature(
                    "TYPE",
                    Feature.newBuilder()
                        .setBytesList(
                            BytesList.newBuilder()
                                .addValue(ByteString.copyFromUtf8(type.name()))
                                .build())
                        .build())
                .build())
        .build();
  }

  /**
   * Convert a set of {@link TSKey#getMinorKeyString() } for a single Major-Key to a Single {@link
   * Example}
   */
  @Experimental
  public static class ColoaseMajorKeyDataPointsToSingleTFExample
      extends PTransform<PCollection<Iterable<TSAccumSequence>>, PCollection<KV<TSKey, Example>>> {

    private static final Logger LOG =
        LoggerFactory.getLogger(ColoaseMajorKeyDataPointsToSingleTFExample.class);

    String fileBase;
    Integer timesteps;

    public ColoaseMajorKeyDataPointsToSingleTFExample(String fileBase, Integer timesteps) {
      this.fileBase = fileBase;
      this.timesteps = timesteps;
    }

    public ColoaseMajorKeyDataPointsToSingleTFExample(
        @Nullable String name, String fileBase, Integer timesteps) {
      super(name);
      this.fileBase = fileBase;
      this.timesteps = timesteps;
    }

    @Override
    public PCollection<KV<TSKey, Example>> expand(PCollection<Iterable<TSAccumSequence>> input) {

      PCollectionTuple exampleAndMetadata = input.apply(createFeaturesFromIterableAccum(timesteps));

      //     Send Metadata file to output location

      exampleAndMetadata
          .get(TIME_SERIES_FEATURE_METADATA)
          .apply(new CreateTFRecordMetadata(fileBase));

      // Process Example
      return exampleAndMetadata.get(TIME_SERIES_EXAMPLES).apply(ParDo.of(new ExampleToKeyValue()));
    }

    public static class ExampleToKeyValue extends DoFn<Example, KV<TSKey, Example>> {

      @ProcessElement
      public void processElement(ProcessContext c, @Element Example example) {

        // Generate TSKey

        Map<String, Feature> featureMap = example.getFeatures().getFeatureMap();

        com.google.protobuf.Timestamp startSpan =
            Timestamps.fromMillis(
                featureMap
                    .get(ExampleMetadata.METADATA_SPAN_START_TS.name())
                    .getInt64List()
                    .getValue(0));
        com.google.protobuf.Timestamp endSpan =
            Timestamps.fromMillis(
                featureMap
                    .get(ExampleMetadata.METADATA_SPAN_END_TS.name())
                    .getInt64List()
                    .getValue(0));

        String majorKey = String.format("%s-%s", startSpan.toString(), endSpan.toString());

        TSKey key = TSKey.newBuilder().setMajorKey(majorKey).build();

        c.output(KV.of(key, example));
      }
    }
  }
}
