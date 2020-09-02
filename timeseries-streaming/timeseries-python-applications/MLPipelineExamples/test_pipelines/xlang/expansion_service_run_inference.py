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

import argparse
import logging
import signal
import sys
import typing

import grpc
from past.builtins import unicode

import apache_beam as beam
from apache_beam.coders import StrUtf8Coder
from apache_beam.pipeline import PipelineOptions
from apache_beam.portability.api import beam_expansion_api_pb2_grpc
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload
from apache_beam.runners.portability import expansion_service
from apache_beam.transforms import ptransform
from apache_beam.utils.thread_pool_executor import UnboundedThreadPoolExecutor
from timeseries.transforms import process_inference_return
from tfx_bsl.public.beam import RunInference
from tfx_bsl.public.proto import model_spec_pb2

import tensorflow as tf

_LOGGER = logging.getLogger(__name__)

RUN_INFERENCE_URN = "beam:transforms:xlang:tfx:run_inference"

@ptransform.PTransform.register_urn(RUN_INFERENCE_URN, bytes)
class RunXlangInference(ptransform.PTransform):
    def __init__(self, saved_model_file):
        self._saved_model_file = saved_model_file

    def expand(self, pcoll):

        _ = (pcoll | 'ParseExamples' >> beam.Map(tf.train.Example.FromString)
             | RunInference(
                    model_spec_pb2.InferenceSpecType(
                        saved_model_spec=model_spec_pb2.SavedModelSpec(
                            signature_name=['serving_default'],
                            model_path=self._saved_model_file)))
             | beam.ParDo(process_inference_return.ProcessReturn())
             | beam.ParDo(process_inference_return.CheckAnomalous())
             )
        return _

    def to_runner_api_parameter(self, unused_context):
        return b'payload', self.payload.encode('utf8')

    @staticmethod
    def from_runner_api_parameter(
            unused_ptransform, payload, unused_context):
        print(parse_string_payload(payload))
        return RunXlangInference(parse_string_payload(payload)['data'])


def parse_string_payload(input_byte):
    payload = ExternalConfigurationPayload()
    payload.ParseFromString(input_byte)
    coder = StrUtf8Coder()
    return {
        k: coder.decode_nested(v.payload)
        for k,
            v in payload.configuration.items()
    }


server = None


def cleanup(unused_signum, unused_frame):
    _LOGGER.info('Shutting down expansion service.')
    server.stop(None)


def main(unused_argv):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-p', '--port', type=int, help='port on which to serve the job api')
    options = parser.parse_args()
    global server
    server = grpc.server(UnboundedThreadPoolExecutor())
    beam_expansion_api_pb2_grpc.add_ExpansionServiceServicer_to_server(
        expansion_service.ExpansionServiceServicer(
            PipelineOptions(
                ["--experiments", "beam_fn_api", "--sdk_location", "container"])),
        server)
    server.add_insecure_port('localhost:{}'.format(options.port))
    server.start()
    _LOGGER.info('Listening for expansion requests at %d', options.port)

    signal.signal(signal.SIGTERM, cleanup)
    signal.signal(signal.SIGINT, cleanup)
    # blocking main thread forever.
    signal.pause()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main(sys.argv)
