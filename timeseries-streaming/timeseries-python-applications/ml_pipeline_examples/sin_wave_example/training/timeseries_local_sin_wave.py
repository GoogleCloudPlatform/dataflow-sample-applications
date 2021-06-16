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

import os
from os.path import join

from tfx.orchestration.beam.beam_dag_runner import BeamDagRunner
from tfx.proto import trainer_pb2

import ml_pipeline.timeseries.pipeline_templates.timeseries_pipeline as pipeline
from tfx.orchestration import metadata
from absl import logging

import ml_pipeline_examples.sin_wave_example.config as config
from ml_pipeline.timeseries.utils import timeseries_transform_utils


def run():
    """
    The sample data produced by the Java pipeline has 42K examples.
    The data is a simple repeating sin-wave.
    """

    # Print map of features that will be generated.
    timeseries_transform_utils.print_feature_pos(config=config.MODEL_CONFIG)

    tfx_pipeline = pipeline.create_pipeline(
            pipeline_name=config.PIPELINE_NAME,
            enable_cache=False,
            run_fn='ml_pipeline.timeseries.encoder_decoder.encoder_decoder_run_fn.run_fn',
            preprocessing_fn=
            'ml_pipeline.timeseries.encoder_decoder.encoder_decoder_preprocessing.preprocessing_fn',
            data_path=config.SYNTHETIC_DATASET['local-raw'],
            pipeline_root=config.LOCAL_PIPELINE_ROOT,
            serving_model_dir=join(config.LOCAL_PIPELINE_ROOT, os.pathsep),
            train_args=trainer_pb2.TrainArgs(num_steps=1680),
            eval_args=trainer_pb2.EvalArgs(num_steps=56),
            metadata_connection_config=metadata.
            sqlite_metadata_connection_config(
                    os.path.join(
                            config.LOCAL_PIPELINE_ROOT,
                            'metadata',
                            config.PIPELINE_NAME,
                            'metadata.db')),
            beam_pipeline_args=['--runner=Directrunner'],
            trainer_custom_config={
                    'train_batches': 500,
                    'eval_batches': 250,
                    'training_example_count': 28000,
                    'eval_example_count': 14000,
                    'timesteps': config.MODEL_CONFIG['timesteps'],
                    'number_features': 3,
                    'outer_units': 16,
                    'inner_units': 8
            },
            transformer_custom_config=config.MODEL_CONFIG,
    )
    BeamDagRunner().run(tfx_pipeline)


if __name__ == '__main__':
    logging.set_verbosity(logging.INFO)
    run()
