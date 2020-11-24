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
from typing import Any, Dict, List, Text, Optional
from ml_metadata.proto import metadata_store_pb2
from tfx.components import ExampleValidator, ImportExampleGen, Pusher
from tfx.components import SchemaGen
from tfx.components import StatisticsGen
from tfx.components import Trainer
from tfx.components import Transform
from tfx.components.base import executor_spec
from tfx.components.trainer import executor as trainer_executor
from tfx.extensions.google_cloud_ai_platform.trainer import executor as ai_platform_trainer_executor
from tfx.extensions.google_cloud_ai_platform.pusher import executor as ai_platform_pusher_executor
from tfx.orchestration import pipeline
from tfx.proto import trainer_pb2, pusher_pb2

from tfx.utils.dsl_utils import external_input


def create_pipeline(
        pipeline_name: Text,
        pipeline_root: Text,
        data_path: Text,
        enable_cache: bool,
        preprocessing_fn: Text,
        run_fn: Text,
        train_args: trainer_pb2.TrainArgs,
        eval_args: trainer_pb2.EvalArgs,
        serving_model_dir: Text,
        metadata_connection_config: Optional[
                metadata_store_pb2.ConnectionConfig] = None,
        beam_pipeline_args: Optional[List[Text]] = None,
        ai_platform_training_args: Optional[Dict[Text, Text]] = None,
        ai_platform_serving_args: Optional[Dict[Text, Any]] = None,
        trainer_custom_config: Optional[Dict[Text, Any]] = None,
        transformer_custom_config: Optional[Dict[Text, Any]] = None,
) -> pipeline.Pipeline:
    components = []

    # Brings data into the pipeline or otherwise joins/converts training data.
    example_gen = ImportExampleGen(input=external_input(data_path))
    components.append(example_gen)

    # Computes statistics over data for visualization and example validation.
    statistics_gen = StatisticsGen(examples=example_gen.outputs['examples'])
    components.append(statistics_gen)

    # Generates schema based on statistics files.
    schema_gen = SchemaGen(
            statistics=statistics_gen.outputs['statistics'],
            infer_feature_shape=False)
    components.append(schema_gen)

    # Performs anomaly detection based on statistics and data schema.
    example_validator = ExampleValidator(  # pylint: disable=unused-variable
        statistics=statistics_gen.outputs['statistics'],
        schema=schema_gen.outputs['schema'])
    components.append(example_validator)

    # Performs transformations and feature engineering in training and serving.
    transform = Transform(
            examples=example_gen.outputs['examples'],
            schema=schema_gen.outputs['schema'],
            preprocessing_fn=preprocessing_fn,
            custom_config=transformer_custom_config,
    )
    components.append(transform)

    # Uses user-provided Python function that implements a model using TF-Learn.
    trainer_args = {
            'run_fn': run_fn,
            'transformed_examples': transform.outputs['transformed_examples'],
            'schema': schema_gen.outputs['schema'],
            'transform_graph': transform.outputs['transform_graph'],
            'train_args': train_args,
            'eval_args': eval_args,
            'custom_executor_spec': executor_spec.ExecutorClassSpec(
                    trainer_executor.GenericExecutor),
            'custom_config': trainer_custom_config,
    }

    if ai_platform_training_args is not None:
        trainer_custom_config[ai_platform_trainer_executor.TRAINING_ARGS_KEY] = ai_platform_training_args
        trainer_args.update({
                'custom_executor_spec': executor_spec.ExecutorClassSpec(
                        ai_platform_trainer_executor.GenericExecutor),
                'custom_config': trainer_custom_config
        })
    trainer = Trainer(**trainer_args)
    components.append(trainer)

    pusher_args = {
            'model': trainer.outputs['model'],
            'push_destination': pusher_pb2.PushDestination(
                    filesystem=pusher_pb2.PushDestination.Filesystem(
                            base_directory=serving_model_dir)),
    }

    if ai_platform_serving_args is not None:
        pusher_args.update({
                'custom_executor_spec': executor_spec.ExecutorClassSpec(
                        ai_platform_pusher_executor.Executor),
                'custom_config': {
                        ai_platform_pusher_executor.SERVING_ARGS_KEY: ai_platform_serving_args
                },
        })
    pusher = Pusher(**pusher_args)  # pylint: disable=unused-variable
    components.append(pusher)

    return pipeline.Pipeline(
            pipeline_name=pipeline_name,
            pipeline_root=pipeline_root,
            components=components,
            enable_cache=enable_cache,
            metadata_connection_config=metadata_connection_config,
            beam_pipeline_args=beam_pipeline_args,
    )
