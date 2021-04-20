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
import com.google.dataflow.sample.retail.businesslogic.core.transforms.DeadLetterSink;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.DeadLetterSink.SinkType;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.ErrorMsg;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.validation.EventDateTimeCorrectionService;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.validation.EventItemCorrectionService;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.validation.ValidateEventDateTime;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.validation.ValidateEventItems;
import com.google.dataflow.sample.retail.businesslogic.core.transforms.clickstream.validation.ValidationUtils;
import javax.annotation.Nullable;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.sdk.values.TypeDescriptors;

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

  public static final TupleTag<Row> MAIN = new TupleTag<Row>() {};

  public static final TupleTag<ErrorMsg> DEAD_LETTER = new TupleTag<ErrorMsg>() {};

  public static final TupleTag<Row> NEEDS_CORRECTIONS = new TupleTag<Row>() {};

  private final DeadLetterSink.SinkType sinkType;

  public ValidateAndCorrectCSEvt(SinkType sinkType) {
    this.sinkType = sinkType;
  }

  public ValidateAndCorrectCSEvt(@Nullable String name, SinkType sinkType) {
    super(name);
    this.sinkType = sinkType;
  }

  @Override
  public PCollection<Row> expand(PCollection<Row> input) {

    // Chain the validation steps

    Schema wrapperSchema = ValidationUtils.getValidationWrapper(input.getSchema());

    // First we wrap the object
    PCollection<Row> wrappedInput =
        input
            .apply(
                "AddWrapper",
                MapElements.into(TypeDescriptors.rows())
                    .via(x -> Row.withSchema(wrapperSchema).withFieldValue("data", x).build()))
            .setRowSchema(wrapperSchema);

    // Next we chain the object through the validation transforms
    PCollectionTuple validateEventItems =
        wrappedInput.apply(
            "ValidateEventDateTime",
            ParDo.of(new ValidateEventItems())
                .withOutputTags(MAIN, TupleTagList.of(ImmutableList.of(DEAD_LETTER))));

    PCollection<Row> validateItems =
        validateEventItems
            .get(MAIN)
            .setRowSchema(wrapperSchema)
            .apply("ValidateEventItems", ParDo.of(new ValidateEventDateTime()))
            .setRowSchema(wrapperSchema);

    // DeadLetter issues are not fixable
    PCollectionList.of(validateEventItems.get(DEAD_LETTER))
        .apply("FlattenDeadLetter", Flatten.pCollections())
        .apply(DeadLetterSink.createSink(sinkType));

    // Next we chain the items through the correction transforms, if they have errors

    PCollectionTuple validatedCollections =
        validateItems.apply(
            ParDo.of(new ValidationUtils.ValidationRouter())
                .withOutputTags(MAIN, TupleTagList.of(ImmutableList.of(NEEDS_CORRECTIONS))));

    // Fix bad Items
    PCollection<Row> eventItemsFixed =
        validatedCollections
            .get(NEEDS_CORRECTIONS)
            .setRowSchema(wrapperSchema)
            .apply("CorrectEventItems", ParDo.of(new EventItemCorrectionService()))
            .setRowSchema(wrapperSchema);

    // Fix Timestamp
    PCollection<Row> eventDateTimeFixed =
        eventItemsFixed
            .apply("CorrectEventDateTime", ParDo.of(new EventDateTimeCorrectionService()))
            .setRowSchema(wrapperSchema);

    PCollection<Row> extractCleanedRows =
        PCollectionList.of(validatedCollections.get(MAIN).setRowSchema(wrapperSchema))
            .and(eventDateTimeFixed)
            .apply(Flatten.pCollections())
            .apply(MapElements.into(TypeDescriptors.rows()).via(x -> x.getRow("data")))
            .setRowSchema(input.getSchema());

    return extractCleanedRows;
  }
}
