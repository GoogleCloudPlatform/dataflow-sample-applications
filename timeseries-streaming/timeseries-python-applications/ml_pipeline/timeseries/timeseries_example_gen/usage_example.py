# Lint as: python2, python3
# Copyright 2020 Google LLC. All Rights Reserved.
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

"""TFX timeseries template pipeline definition.

This file defines TFX pipeline and various components in the pipeline.
"""
import os
from typing import List, Text, Optional
from ml_metadata.proto import metadata_store_pb2
from tfx.components import SchemaGen
from tfx.components import StatisticsGen
from tfx.orchestration import pipeline
from tfx.orchestration.beam.beam_dag_runner import BeamDagRunner
import tempfile

from tfx.proto import example_gen_pb2

from ml_pipeline.timeseries.timeseries_example_gen.component import TimeseriesExampleGen
from tfx.orchestration import metadata


def create_pipeline(
        pipeline_name: Text,
        pipeline_root: Text,
        data_path: Text,
        enable_cache: bool,
        metadata_connection_config: Optional[
            metadata_store_pb2.ConnectionConfig] = None,
        beam_pipeline_args: Optional[List[Text]] = None
) -> pipeline.Pipeline:
    components = []

    # Brings data into the pipeline or otherwise joins/converts training data.
    proto = example_gen_pb2
    output = proto.Output(
        split_config=example_gen_pb2.SplitConfig(splits=[
            proto.SplitConfig.Split(name='train', hash_buckets=3),
            proto.SplitConfig.Split(name='validate', hash_buckets=2),
            proto.SplitConfig.Split(name='test', hash_buckets=1)
        ]))

    timeseriesexample_gen = TimeseriesExampleGen(input_base=data_path,
                                                 output_config=output,
                                                 output_data_format=example_gen_pb2.PayloadFormat.FORMAT_TF_EXAMPLE)
    components.append(timeseriesexample_gen)

    # Computes statistics over data for visualization and example validation.
    statistics_gen = StatisticsGen(examples=timeseriesexample_gen.outputs['examples'])
    components.append(statistics_gen)

    # Generates schema based on statistics files.
    schema_gen = SchemaGen(
        statistics=statistics_gen.outputs['statistics'],
        infer_feature_shape=False)
    components.append(schema_gen)

    return pipeline.Pipeline(
        pipeline_name=pipeline_name,
        pipeline_root=pipeline_root,
        components=components,
        enable_cache=enable_cache,
        metadata_connection_config=metadata_connection_config,
        beam_pipeline_args=beam_pipeline_args,
    )


pipeline_name = 'usage_example'
data_file = os.path.join(os.path.dirname(__file__), 'testdata', '')
metadata_connection_config = metadata.sqlite_metadata_connection_config(
    os.path.join(temp_dir, 'metadata', pipeline_name, 'metadata.db'))

print(f'Output root is {temp_dir}')

BeamDagRunner().run(create_pipeline(pipeline_name='usage_example',
                                    pipeline_root=temp_dir,
                                    enable_cache=False,
                                    metadata_connection_config=metadata_connection_config,
                                    data_path=data_file))
