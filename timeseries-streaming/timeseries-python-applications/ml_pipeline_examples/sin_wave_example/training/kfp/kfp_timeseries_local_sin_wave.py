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

"""Define KubeflowDagRunner to run the pipeline using Kubeflow."""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
from absl import logging
from sin_wave_example import config

from tfx.orchestration.kubeflow import kubeflow_dag_runner
from tfx.proto import trainer_pb2
from tfx.utils import telemetry_utils

# TFX pipeline produces many output files and metadata. All output data will be
# stored under this OUTPUT_DIR.
from timeseries.pipeline_templates import timeseries_pipeline

OUTPUT_DIR = os.path.join('gs://', config.GCS_BUCKET_NAME)

# TFX produces two types of outputs, files and metadata.
# - Files will be created under PIPELINE_ROOT directory.
PIPELINE_ROOT = config.PIPELINE_ROOT

# The last component of the pipeline, "Pusher" will produce serving model under
# SERVING_MODEL_DIR.
SERVING_MODEL_DIR = os.path.join(PIPELINE_ROOT, 'serving_model')

DATA_PATH = config.DATA_ROOT


def run():
    """Define a kubeflow pipeline."""

    # Metadata config.
    metadata_config = kubeflow_dag_runner.get_default_kubeflow_metadata_config()

    # This pipeline automatically injects the Kubeflow TFX image if the
    # environment variable 'KUBEFLOW_TFX_IMAGE' is defined.
    tfx_image = os.environ.get('KUBEFLOW_TFX_IMAGE', None)

    runner_config = kubeflow_dag_runner.KubeflowDagRunnerConfig(
            kubeflow_metadata_config=metadata_config, tfx_image=tfx_image)
    pod_labels = kubeflow_dag_runner.get_default_pod_labels()
    pod_labels.update({telemetry_utils.LABEL_KFP_SDK_ENV: 'tfx-timeseries'})
    kubeflow_dag_runner.KubeflowDagRunner(
            config=runner_config, pod_labels_to_attach=pod_labels
    ).run(
            timeseries_pipeline.create_pipeline(
                    pipeline_name=config.PIPELINE_NAME,
                    enable_cache=True,
                    run_fn=
                    'timeseries.encoder_decoder.encoder_decoder_run_fn.run_fn',
                    preprocessing_fn=
                    'timeseries.encoder_decoder.encoder_decoder_preprocessing.preprocessing_fn',
                    data_path=DATA_PATH,
                    pipeline_root=PIPELINE_ROOT,
                    serving_model_dir=os.path.join(
                            config.PIPELINE_ROOT, os.pathsep),
                    train_args=trainer_pb2.TrainArgs(num_steps=3360),
                    eval_args=trainer_pb2.EvalArgs(num_steps=56),
                    beam_pipeline_args=config.GCP_DATAFLOW_ARGS,
                    trainer_custom_config={
                            'train_batches': 500,
                            'eval_batches': 250,
                            'training_example_count': 28000,
                            'eval_example_count': 14000,
                            'timesteps': config.MODEL_CONFIG['timesteps'],
                            'number_features': 6,
                            'outer_units': 16,
                            'inner_units': 4
                    },
                    transformer_custom_config=config.MODEL_CONFIG,
            ))


if __name__ == '__main__':
    logging.set_verbosity(logging.INFO)
    run()
