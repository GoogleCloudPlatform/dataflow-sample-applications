# Lint as: python2, python3
# Copyright 2019 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Generic TFX CSV example gen executor."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import math
from typing import Any, Dict, List, Text, Union, Tuple, Iterable

from absl import logging
import apache_beam as beam
import tensorflow as tf
from apache_beam import pvalue
from tfx.components.example_gen.base_example_gen_executor import BaseExampleGenExecutor
from tfx.proto import example_gen_pb2
from tfx.types import standard_component_specs
from tfx.utils import proto_utils


def import_examples() -> beam.PTransform:
    """PTransform to import records.
        The records are tf.train.Example, tf.train.SequenceExample,
        or serialized proto.
        Args:
          pipeline: Beam pipeline.
          exec_properties: A dict of execution properties.
            - input_base: input dir that contains input data.
          split_pattern: Split.pattern in Input config, glob relative file pattern
            that maps to input files with root directory given by input_base.
        Returns:
          PCollection of records (tf.Example, tf.SequenceExample, or bytes).
    """

    @beam.ptransform_fn
    @beam.typehints.with_input_types(beam.Pipeline)
    @beam.typehints.with_output_types(Union[tf.train.Example,
                                            tf.train.SequenceExample, bytes])
    def _ImportRecord(self, pipeline: beam.Pipeline, exec_properties: Dict[Text, Any], split_pattern: Text):
        output_payload_format = exec_properties.get(
            standard_component_specs.OUTPUT_DATA_FORMAT_KEY)

        serialized_records = (
                pipeline
                # pylint: disable=no-value-for-parameter
                | beam.io.ReadFromTFRecord(file_pattern=split_pattern))
        if output_payload_format == example_gen_pb2.PayloadFormat.FORMAT_PROTO:
            return serialized_records
        elif (output_payload_format ==
              example_gen_pb2.PayloadFormat.FORMAT_TF_EXAMPLE):
            return (serialized_records
                    | 'ToTFExample' >> beam.Map(tf.train.Example.FromString))
        elif (output_payload_format ==
              example_gen_pb2.PayloadFormat.FORMAT_TF_SEQUENCE_EXAMPLE):
            return (serialized_records
                    | 'ToTFSequenceExample' >> beam.Map(
                        tf.train.SequenceExample.FromString))

    return _ImportRecord


class ProcessTimeseriesExamples(beam.PTransform):

    def __init__(self, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.output_collections = ['train', 'validate', 'test']

    def expand(  # pylint: disable=invalid-name
            self, pcoll: beam.pvalue.PCollection) -> Tuple[beam.pvalue.PCollection, beam.pvalue.PCollection]:
        """Read timeseries examples and extract the min/max time line.
        Args:
          pipeline: Beam pipeline.
          exec_properties: A dict of execution properties.
            - first_timestamp_feature_name :
            - last_timestamp_feature_name  :
        Returns:
          Dictionary with keys: timeline_start, timeline_end
        """

        examples, timestamps = (pcoll | 'ExtractTimeline' >>
                                beam.ParDo(CreateTimestampedValues())
                                .with_outputs('timestamps',
                                              main='examples'))

        min_value = (timestamps | beam.CombineGlobally(lambda x: min(x)))
        max_value = (timestamps | beam.CombineGlobally(lambda x: max(x)))

        min_max_values = ([min_value, max_value] | beam.Flatten() | beam.Map(lambda x: (1, x)) | beam.GroupByKey()
                          | beam.Map(lambda x: {'min': min(x[1]), 'max': max(x[1])}))

        split_boundaries = (min_max_values | beam.Map(lambda x: slice_time(min_max=x)))

        keyed_by_split_examples = examples | beam.ParDo(self.GenerateSplits(self),
                                                        split_boundaries=pvalue.AsSingleton(split_boundaries))

        split_collection = keyed_by_split_examples | beam.Partition(lambda values, num_splits: values[0], 3)

        return split_collection, min_max_values

    class GenerateSplits(beam.DoFn):
        """
        Output record into the correct split
        """

        def process(self, element: Union[Tuple[Dict[int, int], Union[tf.train.Example, tf.train.SequenceExample]]]
                    , split_boundaries: List[int]
                    ) -> Tuple[int, Any]:
            time = element[0]['max']

            pos = next((i for i, x in enumerate(split_boundaries) if x >= time), -1)

            if pos >= 0:
                yield pos, element[1]

            yield pvalue.TaggedOutput('out_of_bounds', element[1])


class CreateTimestampedValues(beam.DoFn):

    def process(self, element: tf.train.Example) -> \
            Iterable[Tuple[Dict[int, int], Union[tf.train.Example, tf.train.SequenceExample]]]:
        features = element.features.feature
        first_timestamp = features['first_timestamp'].int64_list.value[0]

        if features['last_timestamp']:
            last_timestamp = features['last_timestamp'].int64_list.value[0]
        else:
            last_timestamp = first_timestamp

        yield pvalue.TaggedOutput('timestamps', first_timestamp)
        yield pvalue.TaggedOutput('timestamps', last_timestamp)

        yield {'min': first_timestamp, 'max': last_timestamp}, element


def _GenerateExamplesByBeam(
        self,
        pipeline: beam.Pipeline,
        exec_properties: Dict[str, Any],
) -> Dict[str, beam.pvalue.PCollection]:
    """Converts input source to serialized record splits based on configs.
    Custom ExampleGen executor should provide GetInputSourceToExamplePTransform
    for converting input split to serialized records. Overriding this
    'GenerateExamplesByBeam' method instead if complex logic is need, e.g.,
    custom spliting logic.
    Args:
      pipeline: Beam pipeline.
      exec_properties: A dict of execution properties. Depends on detailed
        example gen implementation.
        - input_base: an external directory containing the data files.
        - input_config: JSON string of example_gen_pb2.Input instance, providing
          input configuration.
        - output_config: JSON string of example_gen_pb2.Output instance,
          providing output configuration.
        - output_data_format: Payload format of generated data in output
          artifact, one of example_gen_pb2.PayloadFormat enum.
    Returns:
      Dict of beam PCollection with split name as key, each PCollection is a
      single output split that contains serialized records.
    """

    # Get input split information.
    input_base = exec_properties[standard_component_specs.INPUT_BASE_KEY]

    input_config = example_gen_pb2.Input()
    proto_utils.json_to_proto(
        exec_properties[standard_component_specs.INPUT_CONFIG_KEY],
        input_config)

    # Get output split information.
    output_config = example_gen_pb2.Output()
    proto_utils.json_to_proto(
        exec_properties[standard_component_specs.OUTPUT_CONFIG_KEY],
        output_config)
    # Get output split names.

    # Make beam_pipeline_args available in exec_properties since certain
    # example_gen executors need this information.
    exec_properties['_beam_pipeline_args'] = self._beam_pipeline_args or []


    input_to_record = (
        pipeline |
        self.GetInputSourceToExamplePTransform()(pipeline, exec_properties, input_base))
    example_splits_names = ['train', 'validate', 'test']
    processed_records, min_max = input_to_record | ProcessTimeseriesExamples()

    result = {}
    for index, example_split in enumerate(processed_records):
        result[example_splits_names[index]] = example_split | f'extract_split_{index}' >> beam.Values()

    return result


def slice_time(min_max: Dict[str, int],
               train_ratio: int = 3,
               validation_ratio: int = 1,
               test_ratio: int = 1) -> Dict[str, Tuple[int, int]]:
    """
    Given a Dict with keys [min,max] create a sliced timeline based on the configuration.
    Default time slice is base on creating a ratio of train/validate/test of 3/1/1 along the Timeline with
    smallest granularity of 1ms.
    """

    min_time_unit_ms = 1

    # Determine total time units
    time_segments = train_ratio + test_ratio + validation_ratio

    # Check min max / accommodates number of time_units based on smallest time slice

    length = min_max['max'] - min_max['min']

    if length / time_segments < 1:
        raise ValueError(f"The Timeline starting at timestamp {min_max['min']} "
                         f"and ending at timestamp {min_max['max']} is not divisible by the time_unit count of {time_segments} "
                         f"at 1 ms granularity")

    time_unit_ms = math.ceil(length / time_segments)

    # Determine boundaries for each fold
    min_time = min_max['min']

    train_max_time = min_time + (time_unit_ms * train_ratio) if (train_ratio > 0) else -1
    validation_max_time = min_time + (time_unit_ms * (train_ratio + validation_ratio)) if (train_ratio > 0) else -1
    test_max_time = min_time + (time_unit_ms * (train_ratio + validation_ratio + test_ratio)) \
        if (train_ratio > 0) else -1

    boundaries = [train_max_time, validation_max_time, test_max_time]

    return boundaries


class Executor(BaseExampleGenExecutor):
    """Generic TFX import example gen executor."""

    def GetInputSourceToExamplePTransform(self) -> beam.PTransform:
        """Returns PTransform for importing records."""
        return import_examples()

    def GenerateExamplesByBeam(
            self,
            pipeline: beam.Pipeline,
            exec_properties: Dict[Text, Any],
    ) -> Dict[Text, beam.pvalue.PCollection]:
        return _GenerateExamplesByBeam(self, pipeline, exec_properties)
