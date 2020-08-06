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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkNotNull;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Lists;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSDataPoint;
import com.google.dataflow.sample.timeseriesflow.TimeSeriesData.TSKey;
import com.google.dataflow.sample.timeseriesflow.common.LatestEventTimeDataPoint;
import com.google.dataflow.sample.timeseriesflow.common.TSDataUtils;
import com.google.protobuf.util.Timestamps;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.BigEndianLongCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.ListCoder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.TimerMap;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.DefaultTrigger;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
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
 * <p>{@link PerfectRectangles#ttlDuration()} is the duration after the last value for a key is seen
 * that the gap filling will be completed for. {@link PerfectRectangles#absoluteStopTime()} if this
 * is set then the gap filling will stop at exactly this time. This is useful for bootstrap
 * pipelines.
 *
 * <p>TODO: Create Side Output to capture late data
 */
@Experimental
@AutoValue
public abstract class PerfectRectangles
    extends PTransform<PCollection<KV<TSKey, TSDataPoint>>, PCollection<KV<TSKey, TSDataPoint>>> {

  private static final Logger LOG = LoggerFactory.getLogger(PerfectRectangles.class);

  abstract Duration fixedWindow();

  abstract @Nullable Duration ttlDuration();

  abstract @Nullable Instant absoluteStopTime();

  abstract Boolean enableHoldAndPropogate();

  abstract Builder toBuilder();

  public static Builder builder() {
    return new AutoValue_PerfectRectangles.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setFixedWindow(Duration value);

    public abstract Builder setTtlDuration(Duration value);

    public abstract Builder setAbsoluteStopTime(Instant value);

    public abstract Builder setEnableHoldAndPropogate(Boolean value);

    public abstract PerfectRectangles build();
  }

  public static PerfectRectangles withWindowAndTTLDuration(
      Duration windowDuration, Duration ttlDuration) {
    checkNotNull(windowDuration);
    return new AutoValue_PerfectRectangles.Builder()
        .setFixedWindow(windowDuration)
        .setTtlDuration(ttlDuration)
        .setEnableHoldAndPropogate(false)
        .build();
  }

  public static PerfectRectangles withWindowAndAbsoluteStop(
      Duration windowDuration, Instant absoluteStop) {
    checkNotNull(windowDuration);
    return new AutoValue_PerfectRectangles.Builder()
        .setFixedWindow(windowDuration)
        .setAbsoluteStopTime(absoluteStop)
        .setEnableHoldAndPropogate(false)
        .build();
  }

  public PerfectRectangles enablePreviousValueFill() {
    return this.toBuilder().setEnableHoldAndPropogate(true).build();
  }

  @Override
  public PCollection<KV<TSKey, TSDataPoint>> expand(PCollection<KV<TSKey, TSDataPoint>> input) {

    // Apply fixed window domain and combine to get the last known value in Event time.

    PCollection<KV<TSKey, TSDataPoint>> windowedInput =
        input.apply(Window.into(FixedWindows.of(fixedWindow())));

    PCollection<KV<TSKey, TSDataPoint>> lastValueInWindow =
        windowedInput.apply(LatestEventTimeDataPoint.perKey());

    // Move into Global Time Domain, this allows Keyed State to retain its value across windows.
    // Late Data is dropped at this stage.

    PCollection<KV<TSKey, ValueInSingleWindow<TSDataPoint>>> globalWindow =
        lastValueInWindow
            .apply(Reify.windowsInValue())
            .apply(
                "Global Window With Process Time output.",
                Window.<KV<TSKey, ValueInSingleWindow<TSDataPoint>>>into(new GlobalWindows())
                    .triggering(Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()))
                    .discardingFiredPanes()
                    .withAllowedLateness(Duration.ZERO));

    //     Ensure all output is ordered and gaps are filed and previous values propagated

    PCollection<KV<TSKey, TSDataPoint>> gapCorrectedDataPoints =
        globalWindow
            .apply(ParDo.of(FillGaps.create(this)))
            .apply(
                Window.<KV<TSKey, TSDataPoint>>into(FixedWindows.of(fixedWindow()))
                    .triggering(DefaultTrigger.of()));

    // Add back the filled gap items with the main flows

    PCollection<KV<TSKey, TSDataPoint>> readyForProcessing =
        PCollectionList.of(gapCorrectedDataPoints).and(windowedInput).apply(Flatten.pCollections());

    return readyForProcessing;
  }

  /**
   * This class expects a single data point per key per window from the processing before the Global
   * Window.
   *
   * <p>New Data
   *
   * <p>--------
   *
   * <p>New data is added to a bag state. When a value is seen for first time, this will initiate a
   * event time looping timer for that key. This will cause a Event timer to fire at the lower
   * boundary of every fixed window, even though we are in a Global Window. The Processing time
   * trigger will allow elements to be sent out of the window On the timer firing the Bag state is
   * checked for an event within the current_event_time - Duration value. If there are no values
   * then a new TSDataPoint is created and sent out. If there is a value, nothing is done other than
   * GC.
   *
   * <p>GC
   *
   * <p>----
   *
   * <p>The current GC is a naive implementation which is known to be in-efficient. TODO use Start
   * Bundle / Finish Bindle to make this more efficent.
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

      if (perfectRectangles.absoluteStopTime() == null && perfectRectangles.ttlDuration() == null) {
        throw new IllegalArgumentException(" Must set one of Absolute Stop Time or TTL Duration");
      }
      return new AutoValue_PerfectRectangles_FillGaps.Builder()
          .setPerfectRectangles(perfectRectangles)
          .build();
    }

    // Setup our state objects

    @StateId("lastKnownValue")
    private final StateSpec<ValueState<KV<TSKey, TSDataPoint>>> lastKnownValue =
        StateSpecs.value(KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class)));

    @StateId("lastTimestampUpperBoundary")
    private final StateSpec<ValueState<Long>> lastTimestampUpperBoundary =
        StateSpecs.value(BigEndianLongCoder.of());

    @StateId("sortedValueList")
    private final StateSpec<ValueState<List<KV<TSKey, TSDataPoint>>>> sortedList =
        StateSpecs.value(
            ListCoder.of(KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class))));

    @StateId("currentTimerValue")
    private final StateSpec<ValueState<Long>> currentTimerValue =
        StateSpecs.value(BigEndianLongCoder.of());

    @StateId("newElementsBag")
    private final StateSpec<BagState<KV<TSKey, TSDataPoint>>> newElementsBag =
        StateSpecs.bag(KvCoder.of(ProtoCoder.of(TSKey.class), ProtoCoder.of(TSDataPoint.class)));

    @TimerFamily("actionTimers")
    private final TimerSpec timer = TimerSpecs.timerMap(TimeDomain.EVENT_TIME);

    /**
     * This is the simple path... A new element is here so we add it to the list of elements and set
     * a timer. Timers are expected to be set for every interval window defined by the fixed window
     * duration. As this DoFn is normally within a Global window we use the information in {@link
     * ValueInSingleWindow}.
     *
     * <p>As order is not guaranteed in a global window, we will need to ensure that we do not rest
     * the timer if the elements timestamp is > the current timer.
     */
    @ProcessElement
    public void processElement(
        ProcessContext c,
        @StateId("newElementsBag") BagState<KV<TSKey, TSDataPoint>> newElementsBag,
        @StateId("lastTimestampUpperBoundary") ValueState<Long> lastTimestampUpperBoundary,
        @StateId("currentTimerValue") ValueState<Long> currentTimerValue,
        @TimerFamily("actionTimers") TimerMap timers) {

      lastTimestampUpperBoundary.readLater();
      currentTimerValue.readLater();

      // Add the new TSDataPoint to list of things to be processed on next timer to fire
      newElementsBag.add(KV.of(c.element().getKey(), c.element().getValue().getValue()));

      // Set the timer if it has not already been set
      // The timer needs to fire in the lowest upper window boundary for elements in the Bag
      Instant upperWindowBoundary = c.element().getValue().getWindow().maxTimestamp();
      Long currentAlarm =
          Optional.ofNullable(currentTimerValue.read()).orElse(upperWindowBoundary.getMillis());

      if (currentAlarm >= upperWindowBoundary.getMillis()) {
        timers.set("tick", upperWindowBoundary);
        currentTimerValue.write(upperWindowBoundary.getMillis());
      }

      // Set the value for the highest observed upper boundary timestamp , this will be used to
      // check the TTL on the timer
      Long obeservedTimestamp = c.element().getValue().getWindow().maxTimestamp().getMillis();

      if (Optional.ofNullable(lastTimestampUpperBoundary.read()).orElse(0L) < obeservedTimestamp) {
        lastTimestampUpperBoundary.write(obeservedTimestamp);
      }
    }

    /**
     * This one is a little more complex...
     *
     * <p>There are two activities that happen in the OnTimer event
     *
     * <p>- Processing All current TSAccums in the BagState are read, ordered and added to the
     * processedTimeseriesList. The first item in the list is added to the the lastKnownValue. As
     * long as we have not exceeded the TTL for heartbeats we will set a timer one
     * downsample.duration away from timer.timestamp()
     *
     * <p>- Output We check to see if a processed TSAccum with
     * lowerWindowBoundary==timer.timestamp() exists. If it does exist we will set the previous
     * value to be lastKnownValue and then emit the TSAccum. The lastKnownValue is set to the
     * current value. If no TSAccum exists then we will emit a heartbeat value.
     */
    @OnTimerFamily("actionTimers")
    public void onTimer(
        OnTimerContext c,
        @StateId("newElementsBag") BagState<KV<TSKey, TSDataPoint>> newElementsBag,
        @StateId("currentTimerValue") ValueState<Long> currentTimerValue,
        @StateId("sortedValueList") ValueState<List<KV<TSKey, TSDataPoint>>> sortedList,
        @StateId("lastTimestampUpperBoundary") ValueState<Long> lastTimestampUpperBoundary,
        @TimerFamily("actionTimers") TimerMap timer,
        @StateId("lastKnownValue") ValueState<KV<TSKey, TSDataPoint>> lastKnownValue) {

      newElementsBag.readLater();
      currentTimerValue.readLater();
      sortedList.readLater();
      lastTimestampUpperBoundary.readLater();
      lastKnownValue.readLater();

      // Check if we have more than one value in our list.
      // There are two important considerations why this memory loading should not ordinarily be a
      // factor for the library
      // 1- We only use the Latest values within a window.
      // 2- The norm is for the library to be running in stream mode.
      // This does however mean in batch mode this value will have size == bytesPerElement * Total
      // windows
      List<KV<TSKey, TSDataPoint>> newElements =
          Optional.ofNullable(sortedList.read()).orElse(new ArrayList<>());

      newElements.addAll(Lists.newArrayList(newElementsBag.read()));

      // Clear the elements now that they have been added to memory.
      newElementsBag.clear();

      // Sort the values
      newElements.sort(Comparator.comparing(x -> Timestamps.toMillis(x.getValue().getTimestamp())));

      // Check if there are any values within the current timers search range

      // Alarms are set to fire at the end of a boundary window, so the duration is our range
      Long lowerBoundarySearchWindow =
          c.timestamp().minus(perfectRectangles().fixedWindow()).getMillis();

      Long upperBoundarySearchWindow = c.timestamp().getMillis();

      KV<TSKey, TSDataPoint> lastDatePointSeenInEventTime = null;
      boolean valueWithinSearchWindow = false;

      List<KV<TSKey, TSDataPoint>> keepList = new ArrayList<>();

      lastDatePointSeenInEventTime = lastKnownValue.read();

      Iterator<KV<TSKey, TSDataPoint>> iterator = newElements.iterator();

      while (iterator.hasNext()) {
        KV<TSKey, TSDataPoint> data = iterator.next();

        int compareToLowerWindow =
            Long.compare(
                Timestamps.toMillis(data.getValue().getTimestamp()), lowerBoundarySearchWindow);

        LOG.debug("Lower Window " + Instant.ofEpochMilli(lowerBoundarySearchWindow));

        int compareToUpperWindow =
            Long.compare(
                Timestamps.toMillis(data.getValue().getTimestamp()), upperBoundarySearchWindow);

        LOG.debug("Upper Window " + Instant.ofEpochMilli(upperBoundarySearchWindow));

        // If the value is larger than search lower boundary of window, add the value back for
        // later.
        if (compareToUpperWindow >= 0) {
          LOG.debug("Got one bigger! " + Timestamps.toString(data.getValue().getTimestamp()));
          keepList.add(data);
          break;
        }

        if (compareToLowerWindow >= 0) {
          LOG.debug(
              "Yup there is a value in the window ! "
                  + Timestamps.toString(data.getValue().getTimestamp()));

          // Set current to last value seen
          lastDatePointSeenInEventTime = data;
          valueWithinSearchWindow = true;
          break;
        }
      }

      iterator.forEachRemaining(keepList::add);

      lastKnownValue.write(lastDatePointSeenInEventTime);

      // If there was no match, then we will create a new tick of the correct type.

      if (!valueWithinSearchWindow) {

        LOG.debug("Yup making one up now..");

        if (perfectRectangles().enableHoldAndPropogate()) {
          c.output(
              KV.of(
                  lastDatePointSeenInEventTime.getKey(),
                  lastDatePointSeenInEventTime
                      .getValue()
                      .toBuilder()
                      .setIsAGapFillMessage(true)
                      .setTimestamp(Timestamps.fromMillis(c.timestamp().getMillis()))
                      .build()));

        } else {

          c.output(
              KV.of(
                  lastDatePointSeenInEventTime.getKey(),
                  lastDatePointSeenInEventTime
                      .getValue()
                      .toBuilder()
                      .setIsAGapFillMessage(true)
                      .setData(
                          TSDataUtils.getZeroValueForType(
                              lastDatePointSeenInEventTime.getValue().getData()))
                      .setTimestamp(Timestamps.fromMillis(c.timestamp().getMillis()))
                      .build()));
        }
      }

      sortedList.write(keepList);

      boolean setNewTimer = false;

      // Either absolute time or ttl duration must be set

      if (perfectRectangles().absoluteStopTime() != null) {
        setNewTimer = c.timestamp().isBefore(perfectRectangles().absoluteStopTime());
      }

      // Check if this timer has already passed the time to live duration since the last call.
      if (perfectRectangles().ttlDuration() != null) {

        Instant ttlEnd =
            new Instant(lastTimestampUpperBoundary.read()).plus(perfectRectangles().ttlDuration());
        setNewTimer = c.timestamp().isBefore(ttlEnd);
      }

      if (setNewTimer) {
        Instant nextAlarm =
            new Instant(currentTimerValue.read()).plus(perfectRectangles().fixedWindow());
        LOG.debug("Setting a new timer at " + nextAlarm);
        timer.set("tick", nextAlarm);
        currentTimerValue.write(nextAlarm.getMillis());
      } else {
        currentTimerValue.clear();
      }
    }
  }
}
