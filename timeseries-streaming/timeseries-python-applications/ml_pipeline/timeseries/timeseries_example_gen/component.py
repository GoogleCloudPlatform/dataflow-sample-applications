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
"""TFX CsvExampleGen component definition."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from typing import Optional, Text, Union

from tfx.components.example_gen import component
from ml_pipeline.timeseries.timeseries_example_gen.executor import Executor
from tfx.dsl.components.base import executor_spec
from tfx.orchestration import data_types
from tfx.proto import example_gen_pb2
from tfx.proto import range_config_pb2


class TimeseriesExampleGen(component.FileBasedExampleGen):  # pylint: disable=protected-access
    """TimeseriesExampleGen component.

    The TimeseriesExampleGen component takes TFRecord time-series data, and generates train
    and eval examples for downstream components, ensuring that train and eval data are separated based on time.

    The library can work with both TF.Example and TF.SequenceExample proto's. Each proto is expected to have two
    metadata features:

    first_timestamp :
    This is the timestamp of the earliest data point in the sample.
    The feature name which corresponds to first_timestamp is passed in as a parameter to the component,
    `first_timestamp_name`

    last_timestamp :
    This is the timestamp of the last data point in the sample, if this value does not exist or is null
    then it will be assumed that this element has only one data point at time equal to the first timestamp
    The feature name which corresponds to last_timestamp is passed in as a parameter to the component,
    `last_timestamp_name`

    The split between train and eval is based on the percentage value passed into the component which indicates the
    percentage of time that is used in each split. For example given a data set with timestamps of [0,100) 40% would
    correspond to floor(0.4*100).

    """

    EXECUTOR_SPEC = executor_spec.BeamExecutorSpec(Executor)

    def __init__(
            self,
            input_base: Optional[Text] = None,
            input_config: Optional[Union[example_gen_pb2.Input,
                                         data_types.RuntimeParameter]] = None,
            output_config: Optional[Union[example_gen_pb2.Output,
                                          data_types.RuntimeParameter]] = None,
            range_config: Optional[Union[range_config_pb2.RangeConfig,
                                         data_types.RuntimeParameter]] = None,
            output_data_format: Optional[int] = example_gen_pb2.FORMAT_TF_EXAMPLE):
        """Construct a CsvExampleGen component.

        Args:
          input_base: an external directory containing the TFRecord files.
          input_config: An example_gen_pb2.Input instance, providing input
            configuration. If unset, the files under input_base will be treated as a
            single split.
          output_config: An example_gen_pb2.Output instance, providing output
            configuration. If unset, default splits will be 'train' and 'eval' with
            size 2:1.
          range_config: An optional range_config_pb2.RangeConfig instance,
            specifying the range of span values to consider. If unset, driver will
            default to searching for latest span with no restrictions.
        """
        super(TimeseriesExampleGen, self).__init__(
            input_base=input_base,
            input_config=input_config,
            output_config=output_config,
            output_data_format=output_data_format,
            range_config=range_config)