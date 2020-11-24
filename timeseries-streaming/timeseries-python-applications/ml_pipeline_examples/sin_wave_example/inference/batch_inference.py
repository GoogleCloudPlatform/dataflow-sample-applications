#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import logging
import sys

import tensorflow as tf

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from tfx_bsl.public.beam import RunInference
from tfx_bsl.public.proto import model_spec_pb2
from ml_pipeline.timeseries.encoder_decoder.transforms import process_encdec_inf_rtn
import ml_pipeline_examples.sin_wave_example.config as config


def run(args, pipeline_args):
    """
    Run inference pipeline using data generated from streaming pipeline.
    """
    pipeline_options = PipelineOptions(
            pipeline_args, save_main_session=True, streaming=False)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        _ = (
                pipeline
                | 'ReadTFExample' >> beam.io.tfrecordio.ReadFromTFRecord(
                        file_pattern=args.tfrecord_folder)
                | 'ParseExamples' >> beam.Map(tf.train.Example.FromString)
                | RunInference(
                        model_spec_pb2.InferenceSpecType(
                                saved_model_spec=model_spec_pb2.SavedModelSpec(
                                        signature_name=['serving_default'],
                                        model_path=args.saved_model_location)))
                | beam.ParDo(
                        process_encdec_inf_rtn.ProcessReturn(
                                config={
                                        'tf_transform_graph_dir': args.
                                        tf_transform_graph_dir,
                                        'model_config': config.MODEL_CONFIG
                                }))
                | beam.ParDo(
                        process_encdec_inf_rtn.CheckAnomalous(threshold=0.15))
                | beam.ParDo(print))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
            '--tfrecord_folder',
            dest='tfrecord_folder',
            required=True,
            help=
            'Location of the TFRecord files produced by the streaming pipeline')
    parser.add_argument(
            '--saved_model_location',
            dest='saved_model_location',
            required=True,
            help='location of save model to be used with this inference pipeline'
    )
    parser.add_argument(
            '--tf_transform_graph_dir',
            dest='tf_transform_graph_dir',
            required=True,
            help=
            'location of the tf transform graph dir used in post processing to rescale the values'
    )

    known_args, pipeline_args = parser.parse_known_args()

    run(known_args, pipeline_args)
