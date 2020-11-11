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
import com.google.dataflow.sample.timeseriesflow.TimeSeriesTFExampleKeys.ExampleMetadata;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.Timestamps;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.util.WindowedValue;
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
import org.tensorflow.example.Int64List;

/**
 * Converts a Iterable of {@LINK TSAccum} object into a Example proto with the following structure
 *
 * <p>Metadata
 *
 * <p>{@link ExampleMetadata#METADATA_SPAN_END_TS} Maximum timestamp of the span which the Example
 * covers
 *
 * <p>{@link ExampleMetadata#METADATA_SPAN_START_TS} Minimum timestamp of the span which the Example
 * covers
 *
 * <p>Features are added as MinorKey-Metric.
 *
 * <p>{ feature { key: "__CONFIG_TIMESTEPS-15" value { int64_list { value: 1 } } } feature { key:
 * "value-ABS_MOVING_AVERAGE" value { float_list { value: 61.369998931884766 .... .... } } } }
 *
 * <p>The special internal key __CONFIG_ is a temporary solution that allows configuration to be
 * sent to the TFX transform component. This will be replaced with the option that is coming to the
 * TFX library soon.
 */
public class FeaturesFromIterableAccumSequence
    extends PTransform<PCollection<Iterable<TSAccumSequence>>, PCollectionTuple> {

  private static final Logger LOG =
      LoggerFactory.getLogger(FeaturesFromIterableAccumSequence.class);

  public static final TupleTag<Example> TIME_SERIES_FEATURE_METADATA = new TupleTag<Example>() {};
  public static final TupleTag<Example> TIME_SERIES_EXAMPLES = new TupleTag<Example>() {};

  public Integer timesteps;
  public boolean useMajorKeyInFeature;

  public FeaturesFromIterableAccumSequence(Integer timesteps, boolean useMajorKeyInFeature) {

    this.timesteps = timesteps;
    this.useMajorKeyInFeature = useMajorKeyInFeature;
  }

  public FeaturesFromIterableAccumSequence(@Nullable String name, Integer timesteps) {
    super(name);
    this.timesteps = timesteps;
  }

  @Override
  public PCollectionTuple expand(PCollection<Iterable<TSAccumSequence>> input) {

    return input.apply(
        ParDo.of(new ConvertAllKeyFeaturesToSingleExample(this))
            .withOutputTags(TIME_SERIES_EXAMPLES, TupleTagList.of(TIME_SERIES_FEATURE_METADATA)));
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
                  TIME_SERIES_FEATURE_METADATA,
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
                accumsSequence,
                featuresFromIterableAccumSequence.timesteps,
                featuresFromIterableAccumSequence.useMajorKeyInFeature));

        // Extract the feature names to output as metadata
        for (String key : example.getFeatures().getFeatureMap().keySet()) {
          featureNames.add(
              WindowedValue.<Example>of(
                  TSToTFExampleUtils.createMetadataFeature(
                      key,
                      TSToTFExampleUtils.getTypeFromData(
                          example.getFeatures().getFeatureMap().get(key))),
                  timestamp,
                  window,
                  pc.pane()));
        }

        o.get(TIME_SERIES_EXAMPLES).output(example.build());

      } catch (UnsupportedEncodingException e) {
        LOG.error(e.getMessage());
      }
    }
  }

  /**
   * Returns {@link Features} with order based on sequence time boundaries. If the minor key is the
   * pivot, mainly in iot use cases, then pass true to useMajorKeyInFeature. This will create
   * feature names which are == to metric names.
   */
  public static Features convertAllKeyFeaturesToSingleExample(
      Iterable<TSAccumSequence> accums, Integer timesteps, boolean useMajorKeyInFeatureName)
      throws UnsupportedEncodingException {

    Builder features = Features.newBuilder();

    Long upperBoundary = null;
    Long lowerBoundary = null;

    String majorKey = "";

    List<String> numericFeatureNames = new ArrayList<>();

    // We have multi array of TSAccumSequence, every Sequence is 'feature' as its a different
    // minor key Step We need to convert every TSAccumSequence to an array of [Feature[]]

    for (TSAccumSequence accumSequence : accums) {

      // The feature name is a combined key of MajorKey-MinorKey-Metric
      // For example iotdevice001-batterylevel-MIN
      String timeseriesName = accumSequence.getKey().getMajorKey();

      String featureName = accumSequence.getKey().getMinorKeyString();
      majorKey = accumSequence.getKey().getMajorKey();

      if (useMajorKeyInFeatureName) {
        featureName = String.join("-", timeseriesName, featureName);
      }

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
        Feature.newBuilder().setInt64List(Int64List.newBuilder().addValue(lowerBoundary)).build());

    features.putFeature(
        ExampleMetadata.METADATA_SPAN_END_TS.name(),
        Feature.newBuilder().setInt64List(Int64List.newBuilder().addValue(upperBoundary)).build());

    if (!useMajorKeyInFeatureName) {
      features.putFeature(
          ExampleMetadata.METADATA_MAJOR_KEY.name(),
          Feature.newBuilder()
              .setBytesList(BytesList.newBuilder().addValue(ByteString.copyFromUtf8(majorKey)))
              .build());
    }

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
          TSToTFExampleUtils.tfFeatureFromTSData(map.getValue()));
    }

    // Add all Span wide features
    for (String key : accums.getSequenceDataMap().keySet()) {
      features.putFeature(
          String.format("%s-%s", featurePrefix, key),
          TSToTFExampleUtils.tfFeatureFromTSDataPoint(accums.getSequenceDataMap().get(key)));
    }

    return features;
  }
}
