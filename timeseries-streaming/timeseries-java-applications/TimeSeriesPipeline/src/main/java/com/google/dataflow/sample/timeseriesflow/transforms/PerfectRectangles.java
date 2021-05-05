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
package com.google.dataflow.sample.timeseriesflow.transforms;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.dataflow.sample.timeseriesflow.options.TSFlowOptions;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.CombiningState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.annotations.VisibleForTesting;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This transform takes Time series data points which are in a Fixed Window Time domain. Dependent
 * on options the transform is also able to : - fill any gaps in the data - propagate the previous
 * fixed window value into a generated value for the fixed window.
 *
 * <p>{@link PerfectRectangles#getTtlDuration()} ()} is the duration after the last value for a key
 * is seen that the gap filling will be completed for.
 *
 * <p>{@link PerfectRectangles#getAbsoluteStopTime()} ()} if this is set then the gap filling will
 * stop at exactly this time. This is useful for bootstrap pipelines.
 *
 * <p>{@link PerfectRectangles#enablePreviousValueFill()} enables the use of the last known value as
 * the value to be used to fill gaps.
 *
 * <p>{@link PerfectRectangles#getPreviousValueFillExcludeList()} (List<TSKey>)} An Optional list of
 * TSKey's can be provided which will be excluded from the {@link
 * PerfectRectangles#enablePreviousValueFill()} policy if set. If the TSKey Major Key is not set
 * then the minorkey will be assumed to match all major keys.
 *
 * <p>In order to do the gap filling we make use of known characteristics of the data coming into
 * this transform
 * <li>1- All data is downsampled during type 1 computations using a fixed window. There is no path
 *     through the library which does not have this applied.
 * <li>2- All data in the library is based on the TS.proto, which always has a timestamp as part of
 *     the schema and is a hard requirement.
 *
 *     <p>Latest transform
 * <li>The latest transform provides us with 1 value per fixed window when there is data. This
 *     reduces the amount of work we will need to do downstream and makes use of a Combiner for nice
 *     scaling. A modified version of the Latest transform in Beam was used for this purpose.
 *
 *     <p>Global Window
 * <li>>This transform requires timers to be set in the 'next' fixed window, which is not supported
 *     with the Apache Beam model. Timers can only be set within the current window. So to make this
 *     possible we must make use of the Global Window. In the Global window order is not guaranteed.
 *     So data will need to timestamp sorted.
 *
 *     <p>TODO: Create Side Output to capture late data
 */
@Experimental
@AutoValue
public abstract class PerfectRectangles
    extends PTransform<PCollection<KV<TSKey, TSDataPoint>>, PCollection<KV<TSKey, TSDataPoint>>> {

  private static final Logger LOG = LoggerFactory.getLogger(PerfectRectangles.class);

  abstract Duration getFixedWindowDuration();

  abstract Boolean getEnableHoldAndPropogate();

  abstract @Nullable List<TSKey> getPreviousValueFillExcludeList();

  abstract @Nullable Duration getTtlDuration();

  abstract @Nullable Instant getAbsoluteStopTime();

  abstract @Nullable Instant getSuppressedEarlyValues();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_PerfectRectangles.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFixedWindowDuration(Duration value);

    public abstract Builder setEnableHoldAndPropogate(Boolean value);

    public abstract Builder setPreviousValueFillExcludeList(List<TSKey> excludeList);

    public abstract Builder setTtlDuration(Duration value);

    public abstract Builder setAbsoluteStopTime(Instant value);

    public abstract Builder setSuppressedEarlyValues(Instant values);

    public abstract PerfectRectangles build();
  }

  public static PerfectRectangles fromPipelineOptions(TSFlowOptions options) {
    checkNotNull(options);
    checkArgument(
        !(options.getTTLDurationSecs() == null && options.getAbsoluteStopTimeMSTimestamp() == null),
        "Must set either TTL or absolute stop time.");

    AutoValue_PerfectRectangles.Builder builder = new AutoValue_PerfectRectangles.Builder();

    if (options.getTTLDurationSecs() != null) {

      builder.setTtlDuration(Duration.standardSeconds(options.getTTLDurationSecs()));
    }

    if (options.getAbsoluteStopTimeMSTimestamp() != null) {
      builder.setAbsoluteStopTime(Instant.ofEpochMilli(options.getAbsoluteStopTimeMSTimestamp()));
    }

    builder.setFixedWindowDuration(
        Duration.standardSeconds(options.getTypeOneComputationsLengthInSecs()));

    // Option has a default value of false.
    builder.setEnableHoldAndPropogate(options.getEnableHoldAndPropogateLastValue());

    return builder.build();
  }

  public static PerfectRectangles withWindowAndTTLDuration(
      Duration windowDuration, Duration ttlDuration) {
    checkNotNull(windowDuration);
    return new AutoValue_PerfectRectangles.Builder()
        .setFixedWindowDuration(windowDuration)
        .setTtlDuration(ttlDuration)
        .setEnableHoldAndPropogate(false)
        .build();
  }

  public static PerfectRectangles withWindowAndAbsoluteStop(
      Duration windowDuration, Instant absoluteStop) {
    checkNotNull(windowDuration);
    return new AutoValue_PerfectRectangles.Builder()
        .setFixedWindowDuration(windowDuration)
        .setAbsoluteStopTime(absoluteStop)
        .setEnableHoldAndPropogate(false)
        .build();
  }

  /**
   * Assign the last known value to any gaps. So if last value was a TSDataPoint with Data 8, then
   * the gap filled value with have Data 8 and timestamp from previous value.
   */
  public PerfectRectangles enablePreviousValueFill() {
    return this.toBuilder().setEnableHoldAndPropogate(true).build();
  }

  /**
   * An Optional list of TSKey's can be provided which will be excluded from the {@link
   * PerfectRectangles#enablePreviousValueFill()} policy if set. If the TSKey Major Key is not set
   * then the minorkey will be assumed to match all major keys.
   */
  public PerfectRectangles withPreviousValueFillExcludeList(List<TSKey> excludeList) {
    checkNotNull(excludeList);
    return this.toBuilder().setPreviousValueFillExcludeList(excludeList).build();
  }

  /**
   * Often during batch bootstrap it is desirable to ignore early values where not all known keys
   * have yet had a value. For example there are keys K1, K2 and K3. K1 has a value at 20:00:00, K2
   * at 20:00:01 and K3 at 20:00:02. By setting this value to 20:00:03 all outputs from before this
   * time will be suppressed. Allowing the final output to include all keys.
   */
  public PerfectRectangles suppressEarlyValuesWithStartTime(Instant startTimeForFirstValues) {
    return this.toBuilder().setSuppressedEarlyValues(startTimeForFirstValues).build();
  }

  @Override
  public PCollection<KV<TSKey, TSDataPoint>> expand(PCollection<KV<TSKey, TSDataPoint>> input) {

    // Apply fixed window domain and combine to get the last known value in Event time.

    PCollection<KV<TSKey, TSDataPoint>> windowedInput =
        input.apply(
            "GapFillWindow",
            Window.<KV<TSKey, TSDataPoint>>into(FixedWindows.of(getFixedWindowDuration()))
                .withAllowedLateness(Duration.ZERO));

    PCollection<KV<TSKey, TSDataPoint>> lastValueInWindow =
        // windowedInput.apply(Latest.perKey()LatestEventTimeDataPoint.perKey());
        windowedInput.apply(Latest.perKey());

    // Move into Global Time Domain, this allows Keyed State to retain its value across windows.
    // Late Data is dropped at this stage. Before we apply the Global Window we need to retain the
    // IntervalWindow information for the element. This is done using Reify.

    PCollection<KV<TSKey, ValueInSingleWindow<TSDataPoint>>> globalWindow =
        lastValueInWindow
            .apply(Reify.windowsInValue())
            .apply(
                "Global Window With Process Time output.",
                Window.<KV<TSKey, ValueInSingleWindow<TSDataPoint>>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes());

    //     Ensure all output is ordered and gaps are filed and previous values propagated

    PCollection<KV<TSKey, TSDataPoint>> gapCorrectedDataPoints =
        globalWindow
            .apply(ParDo.of(FillGaps.create(this)))
            .apply(
                "PostGapFillReWindow",
                Window.<KV<TSKey, TSDataPoint>>into(FixedWindows.of(getFixedWindowDuration()))
                    .triggering(DefaultTrigger.of()));

    // Add back the filled gap items with the main flows

    PCollection<KV<TSKey, TSDataPoint>> readyForProcessing =
        PCollectionList.of(gapCorrectedDataPoints).and(windowedInput).apply(Flatten.pCollections());

    // If suppress has been enabled remove early values
    if (getSuppressedEarlyValues() != null) {
      return readyForProcessing.apply(
          Filter.by(
              x ->
                  Timestamps.toMillis(x.getValue().getTimestamp())
                      > getSuppressedEarlyValues().getMillis()));
    }

    return readyForProcessing;
  }

  /**
   * Once we see a key for the first time, we need to do some setup activity in ProcessElement().
   *
   * <p>Start Looping Timers
   * <li>The timer needs to be set in the next intervalWindow that the trailingWM belongs to. This
   *     data is not available to us in the GlobalWindow, however we can have access to this data
   *     before we apply the GlobalWindow by using: Reify.windowsInValue() The first looping timer
   *     to fire must be the fixed window after the minimum(BoundedWindow) seen. Timers do not
   *     support an inbuilt setLowest() value, so we need to create a State to keep track of this
   *     minimum. We use a State variable called trailingWWM to hold this value for us. When a new
   *     element comes in we compare its value to the trailingWWM and if lower we replace the
   *     trailingWWM value and set the timer. The OnProcess setting of timers should only be at the
   *     first timers in the sequence for the looping timer. It should not override the timer value
   *     which is being set within the OnTimer event. Therefore we will make use of Boolean State
   *     isTimerEnabled
   *
   *     <p>OnTimer()
   * <li>This is the class that does all the actual work.
   * <li>High level: Move the values in the unsorted ElementBag into a list.This requires memory
   *     space == the number of windows 'active' In stream mode this will be small and stream mode
   *     does not store everything in memory . In batch mode this can be as big as (Max(timestamp of
   *     all keys) - Min(timestamp of all keys) / FixedWindow size. For example if we have 6 months
   *     of data and we we have fixed windows of 1 sec (6*30*86400) ~ 16M objects per key.
   * <li>Sort the list based on the timestamp value of the objects. Determine if there is a data
   *     point in this 'window'. Window is defined as current onTimer timestamp + Duration of the
   *     fixed window. If there is a data point, reset the timer to fire in the next interval
   *     window. Save the current TSDataPoint into State in case we need it for data propagation.
   *     Carry out GC by remove the last known value from the Sorted list Store the sorted list into
   *     a ValueState object, which we can reuse on the next timer firing. If there is no data in
   *     this window, generate a GapFill object. Use the last known value or null dependent on the
   *     config the user provided.
   */
  @Experimental
  @AutoValue
  @VisibleForTesting
  public abstract static class FillGaps
      extends DoFn<KV<TSKey, ValueInSingleWindow<TSDataPoint>>, KV<TSKey, TSDataPoint>> {

    public abstract PerfectRectangles perfectRectangles();

    public abstract Builder toBuilder();

    @AutoValue.Builder
    public abstract static class Builder {
      public abstract Builder setPerfectRectangles(PerfectRectangles value);

      public abstract FillGaps build();
    }

    static FillGaps create(PerfectRectangles perfectRectangles) {

      if (perfectRectangles.getAbsoluteStopTime() == null
          && perfectRectangles.getTtlDuration() == null) {
        throw new IllegalArgumentException(" Must set one of Absolute Stop Time or TTL Duration");
      }
      return new AutoValue_PerfectRectangles_FillGaps.Builder()
          .setPerfectRectangles(perfectRectangles)
          .build();
    }

    // Place holder for items that will be processed in the OnProcess calls.
    @StateId("newElementsBag")
    private final StateSpec<BagState<KV<TSKey, TSDataPoint>>> newElementsBag =
        StateSpecs.bag(KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class)));

    // trailingVM is the minimum timer seen. This value can only be moved forward in OnTimer().
    @StateId("trailingWWM")
    private final StateSpec<CombiningState<Long, long[], Long>> trailingWM =
        StateSpecs.combining(
            new Combine.BinaryCombineLongFn() {
              @Override
              public long apply(long left, long right) {
                // If current value is 0 then always use the first non null value.
                if (left == 0) {
                  return right;
                }
                int x = Long.compare(left, right);
                return (x < 0) ? left : right;
              }

              @Override
              public long identity() {
                return 0;
              }
            });

    @StateId("lastStoredValue")
    private final StateSpec<ValueState<KV<TSKey, TSDataPoint>>> lastStoredValue =
        StateSpecs.value(KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class)));

    @StateId("gapStartingTimestamp")
    private final StateSpec<ValueState<Long>> gapStartingTimestamp =
        StateSpecs.value(BigEndianLongCoder.of());

    @StateId("sortedValueList")
    private final StateSpec<ValueState<List<KV<TSKey, TSDataPoint>>>> sortedList =
        StateSpecs.value(
            ListCoder.of(KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class))));

    @StateId("isTimerActive")
    private final StateSpec<ValueState<Boolean>> isTimerActive =
        StateSpecs.value(BooleanCoder.of());

    @TimerFamily("actionTimers")
    private final TimerSpec timer = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    private final List<String> minorKeyExcludeList = new ArrayList<>();

    @Setup
    public void setup() {
      if (perfectRectangles().getPreviousValueFillExcludeList() != null) {
        for (TSKey key : perfectRectangles().getPreviousValueFillExcludeList()) {
          if (key.getMajorKey().equals("")) {
            this.minorKeyExcludeList.add(key.getMinorKeyString());
          }
        }
      }
    }

    /**
     * At most one element is seen per window. This value is never used in the window it arrives in.
     * It is only a signal that a real value was seen in a window, its data point can be reused
     * later on.
     *
     * <p>It is possible that the event timestamp in the data point, is not within the boundaries of
     * the window that it arrived in. This can happen when the source is using process time vs event
     * time, for example when TimestampID is not set for PubSub. The timestamp in the even is
     * therefore reset to be the current window timestamp.
     */
    @ProcessElement
    public void processElement(
        ProcessContext c,
        @Timestamp Instant windowTimestamp,
        @StateId("newElementsBag") BagState<KV<TSKey, TSDataPoint>> newElementsBag,
        @StateId("trailingWWM") CombiningState<Long, long[], Long> trailingWWM,
        @StateId("isTimerActive") ValueState<Boolean> isTimerActive,
        @TimerFamily("actionTimers") TimerMap timers) {

      TSDataPoint timeFixedDataPoint =
          c.element()
              .getValue()
              .getValue()
              .toBuilder()
              .setTimestamp(
                  Timestamps.fromMillis(
                      c.element().getValue().getWindow().maxTimestamp().getMillis() - 1))
              .build();

      // Add the new TSDataPoint to our holding queue
      newElementsBag.add(KV.of(c.element().getKey(), timeFixedDataPoint));

      // Update our trailingWWM, this value can never be NULL.
      trailingWWM.add(c.element().getValue().getWindow().maxTimestamp().getMillis());

      // If the timer is enabled we differ to OnTimer processing for its setting.
      if (!Optional.ofNullable(isTimerActive.read()).orElse(false)) {
        // Set the timer to fire at the end of the trailingWM window, this allows the system to init
        // with a real value.
        timers.set("tick", Instant.ofEpochMilli(trailingWWM.read()));
      }
    }

    /** */
    @OnTimerFamily("actionTimers")
    public void onTimer(
        OnTimerContext c,
        @StateId("newElementsBag") BagState<KV<TSKey, TSDataPoint>> newElementsBag,
        @StateId("sortedValueList") ValueState<List<KV<TSKey, TSDataPoint>>> sortedValueList,
        @StateId("lastStoredValue") ValueState<KV<TSKey, TSDataPoint>> lastStoredValue,
        @StateId("isTimerActive") ValueState<Boolean> isTimerActive,
        @StateId("gapStartingTimestamp") ValueState<Long> gapStartingTimestamp,
        @StateId("trailingWWM") CombiningState<Long, long[], Long> trailingWWM,
        @TimerFamily("actionTimers") TimerMap timer) {

      // Move elements in the sortedElements queue to a List and sort.

      long t = System.currentTimeMillis();

      List<KV<TSKey, TSDataPoint>> sortedElements =
          loadAndSortElements(newElementsBag, sortedValueList);

      // Determine what our current interval window is
      IntervalWindow currentWindowBoundary =
          new IntervalWindow(
              c.timestamp().minus(perfectRectangles().getFixedWindowDuration()),
              perfectRectangles().getFixedWindowDuration());

      // Process elements in the sorted list until gap is found
      KV<IntervalWindow, KV<TSKey, TSDataPoint>> lastValueBeforeGap =
          checkIfValueInBoundary(sortedElements, currentWindowBoundary);

      // If there is no Gap then we set the timer to fire in the  window after the last known value.
      // And ensure the
      // LastValue is updated. Important note the last Value could be in the 'future' as we seek
      // ahead in the event queue for efficiency
      // TODO Change this impl once orderedliststate is added to Beam.

      if (lastValueBeforeGap != null) {
        // Store the current value into State
        lastStoredValue.write(lastValueBeforeGap.getValue());
        // Set a timer in the next window
        timer.set(
            "tick",
            lastValueBeforeGap.getKey().end().plus(perfectRectangles().getFixedWindowDuration()));
        isTimerActive.write(true);
        // Clear any previous gap timestamps
        gapStartingTimestamp.clear();
        // Garbage collection
        sortedElements =
            garbageCollect(sortedElements, lastValueBeforeGap.getKey().end().getMillis());
      }

      // If there is a gap then we will gap fill, using the last known value

      if (lastValueBeforeGap == null) {

        // Load the last known value, this value can be NULL if this is the init state
        KV<TSKey, TSDataPoint> lastValue = lastStoredValue.read();

        if (lastValue == null) {
          // This is a state check, we should never have a situation where lastValue == null and the
          // first value in the sorted lists timestamp is > then current window.
          if (sortedElements.isEmpty()) {

            throw new IllegalStateException(
                String.format(
                    "There is no known last value and no new values in the list for window boundary %s. If you are using the DirectRunner please switch to another runner like Dataflow or Flink, we are investigating a known issue with larger datasets and the direct runner and this class.",
                    currentWindowBoundary.end()));
          }

          if (Timestamps.toMillis(sortedElements.get(0).getValue().getTimestamp())
              > currentWindowBoundary.end().getMillis()) {
            throw new IllegalStateException(
                String.format(
                    "There is no known last value and the current list first value %s timestamp is > the timer window %s.",
                    Timestamps.toString(sortedElements.get(0).getValue().getTimestamp()),
                    currentWindowBoundary.end()));
          }
          lastValue = sortedElements.get(0);
        }

        // Store the last known value in state
        lastStoredValue.write(lastValue);

        // Before we output we need to check if we are within TTL

        boolean withinTTL = checkWithinTT(c, gapStartingTimestamp);

        // GC the old TSDataPoint
        sortedElements = garbageCollect(sortedElements, currentWindowBoundary.end().getMillis());

        if (withinTTL) {

          // Output a GapFill TSDataPoint
          outputGapFill(lastValue, c);

          // If this is the first window in which a gap happens set the gap starting timestamp
          if (gapStartingTimestamp.read() == null) {
            gapStartingTimestamp.write(currentWindowBoundary.end().getMillis());
          }

          // Set timer to fire again as we are within TTL Absolute end time
          Instant nextAlarm =
              new Instant(c.timestamp()).plus(perfectRectangles().getFixedWindowDuration());
          timer.set("tick", nextAlarm);
          isTimerActive.write(true);

        } else {
          // If the sorted elements is now empty and we are TTL we can stop the looping timer &
          // clear all state and end the OnTimer()
          if (sortedElements.isEmpty()) {
            isTimerActive.clear();
            sortedValueList.clear();
            lastStoredValue.clear();
            gapStartingTimestamp.clear();
            trailingWWM.clear();
          } else {

            // As there are data points on the horizon, we need to set a new timer, this needs to
            // match the start of
            // the fixed window periods, similar to the values that came from the LASTVALUE
            // transform.

            Instant nextAlarm =
                new Instant(c.timestamp()).plus(perfectRectangles().getFixedWindowDuration());
            KV<TSKey, TSDataPoint> nextValue = sortedElements.get(0);

            while (nextAlarm.getMillis()
                <= Timestamps.toMillis(nextValue.getValue().getTimestamp())) {
              nextAlarm = nextAlarm.plus(perfectRectangles().getFixedWindowDuration());
            }

            timer.set("tick", nextAlarm);

            lastStoredValue.clear();
            gapStartingTimestamp.clear();
          }

          return;
        }
      }

      // Store the sorted list for next window
      sortedValueList.write(sortedElements);
    }

    private List<KV<TSKey, TSDataPoint>> loadAndSortElements(
        BagState<KV<TSKey, TSDataPoint>> newElementsBag,
        ValueState<List<KV<TSKey, TSDataPoint>>> sortedValueList) {

      List<KV<TSKey, TSDataPoint>> sortedElements =
          Optional.ofNullable(sortedValueList.read()).orElse(new ArrayList<>());

      int sizeOfListBefore = sortedElements.size();

      sortedElements.addAll(Lists.newArrayList(newElementsBag.read()));

      int sizeOfListAfter = sortedElements.size();

      // Clear the elements now that they have been added to memory.
      newElementsBag.clear();

      if (sizeOfListAfter != sizeOfListBefore) {
        // Sort the values, this is a very slow operation
        sortedElements.sort(
            Comparator.comparing(x -> Timestamps.toMillis(x.getValue().getTimestamp())));
      }

      return sortedElements;
    }

    /*
     * TODO Replace with OrderedStateList based implementation when it becomes available.
     *
     */
    private KV<IntervalWindow, KV<TSKey, TSDataPoint>> checkIfValueInBoundary(
        List<KV<TSKey, TSDataPoint>> sortedElements, IntervalWindow currentWindowBoundary) {
      long t = System.currentTimeMillis();

      // If the list is empty return null;
      if (sortedElements.isEmpty()) {
        return null;
      }

      long start = currentWindowBoundary.start().getMillis();
      // Boundary is exclusive of max timestamp
      long end = currentWindowBoundary.end().getMillis();

      // Get an iterator to loop through our sorted list
      Iterator<KV<TSKey, TSDataPoint>> it = sortedElements.iterator();

      // First we check if the current window has a value

      KV<IntervalWindow, KV<TSKey, TSDataPoint>> lastKnownValue = null;

      while (it.hasNext() && lastKnownValue == null) {
        KV<TSKey, TSDataPoint> next = it.next();
        long timestamp = Timestamps.toMillis(next.getValue().getTimestamp());
        // If the timestamp is > then our window then we can stop
        if (timestamp > end) {
          return null;
        }
        // If value is bigger than start then its a valid value for this window.
        if (timestamp >= start) {
          lastKnownValue = KV.of(currentWindowBoundary, next);
        }
      }

      Instant startOfWindow = currentWindowBoundary.end();
      Instant endOfWindow =
          currentWindowBoundary.end().plus(perfectRectangles().getFixedWindowDuration());
      // For efficiency we now seek forward until we find a gap
      while (it.hasNext()) {

        KV<TSKey, TSDataPoint> next = it.next();
        long timestamp = Timestamps.toMillis(next.getValue().getTimestamp());
        if (timestamp >= startOfWindow.getMillis() && timestamp < endOfWindow.getMillis()) {
          lastKnownValue = KV.of(new IntervalWindow(startOfWindow, endOfWindow), next);
        } else {
          break;
        }
        startOfWindow = startOfWindow.plus(perfectRectangles().getFixedWindowDuration());
        endOfWindow = endOfWindow.plus(perfectRectangles().getFixedWindowDuration());
      }

      return lastKnownValue;
    }

    private boolean checkWithinTT(OnTimerContext c, ValueState<Long> gapStartingTimestamp) {

      // Either absolute time or ttl duration are always set

      if (perfectRectangles().getAbsoluteStopTime() != null) {
        return c.timestamp().isBefore(perfectRectangles().getAbsoluteStopTime());
      }

      // If this is the first time a gap value has been created, check if TTL is > 0

      Long gapStart = gapStartingTimestamp.read();

      if (gapStart == null) {
        return perfectRectangles().getTtlDuration().getMillis() > 0;
      }

      return c.timestamp()
          .isBefore(new Instant(gapStart).plus(perfectRectangles().getTtlDuration()));
    }

    private void outputGapFill(KV<TSKey, TSDataPoint> lastValue, OnTimerContext c) {

      // Check if we have a last known value, if its Null, then the first value from our sorted list
      // is the value.

      if (perfectRectangles().getEnableHoldAndPropogate()
          && checkIfValueNotInExcludeList(lastValue.getKey())) {
        c.output(
            KV.of(
                lastValue.getKey(),
                lastValue
                    .getValue()
                    .toBuilder()
                    .setIsAGapFillMessage(true)
                    .setTimestamp(Timestamps.fromMillis(c.timestamp().getMillis()))
                    .build()));

      } else {

        c.output(
            KV.of(
                lastValue.getKey(),
                lastValue
                    .getValue()
                    .toBuilder()
                    .setIsAGapFillMessage(true)
                    .setData(TSDataUtils.getZeroValueForType(lastValue.getValue().getData()))
                    .setTimestamp(Timestamps.fromMillis(c.timestamp().getMillis()))
                    .build()));
      }
    }

    /** Remove all values before the end of the current window */
    @VisibleForTesting
    public List<KV<TSKey, TSDataPoint>> garbageCollect(
        List<KV<TSKey, TSDataPoint>> sortedList, long windowEnd) {

      if (sortedList.isEmpty()) {
        return sortedList;
      }

      int pos = -1;

      for (ListIterator<KV<TSKey, TSDataPoint>> iter = sortedList.listIterator();
          iter.hasNext(); ) {
        if (Timestamps.toMillis(iter.next().getValue().getTimestamp()) > windowEnd) {
          pos = iter.previousIndex();
          break;
        }
      }

      // We found a value which is larger than the window , so create sublist of the values
      if (pos >= 0) {
        sortedList.subList(0, pos).clear();
        return sortedList;
      }

      sortedList.clear();

      return sortedList;
    }

    @VisibleForTesting
    public boolean checkIfValueNotInExcludeList(TSKey key) {

      if (perfectRectangles().getPreviousValueFillExcludeList() == null) {
        return true;
      }

      return !perfectRectangles().getPreviousValueFillExcludeList().contains(key)
          && !minorKeyExcludeList.contains(key.getMinorKeyString());
    }
  }
}
