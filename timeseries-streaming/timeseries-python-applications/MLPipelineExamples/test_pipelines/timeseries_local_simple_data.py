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

import timeseries.pipeline_templates.timeseries_pipeline as pipeline
from tfx.orchestration import metadata

import config

PIPELINE_NAME = 'synthetic_data_pipeline'

tfx_pipeline = pipeline.create_pipeline(
    pipeline_name=PIPELINE_NAME,
    enable_cache=False,
    run_fn='timeseries.encoder_decoder.encoder_decoder_run_fn.run_fn',
    preprocessing_fn=
    'timeseries.encoder_decoder.encoder_decoder_preprocessing.preprocessing_fn',
    data_path=config.SYNTHETIC_DATASET['local-raw'],
    pipeline_root=config.PIPELINE_ROOT,
    serving_model_dir=join(config.PIPELINE_ROOT, os.pathsep),
    train_args=trainer_pb2.TrainArgs(num_steps=280),
    eval_args=trainer_pb2.EvalArgs(num_steps=140),
    metadata_connection_config=metadata.sqlite_metadata_connection_config(
        os.path.join(config.PIPELINE_ROOT, 'metadata', PIPELINE_NAME, 'metadata.db')),
    beam_pipeline_args=['--runner=Directrunner'],
    trainer_custom_config={'epochs': 30,
                           'train_batches': 1000,
                           'eval_batches': 1000,
                           'timesteps': 5,
                           'number_features': 2,
                           'outer_units': 16,
                           'inner_units': 4})
BeamDagRunner().run(tfx_pipeline)
