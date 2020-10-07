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
import functools
import math
import unittest
import numpy
from datetime import datetime
import tensorflow as tf
import timeseries.encoder_decoder.encoder_decoder_preprocessing as encoder_decoder_preprocessing
from scipy.stats import stats
from tensorflow_transform.beam import tft_unit
from tensorflow_transform.beam import impl as beam_impl
from timeseries.utils import timeseries_transform_utils as ts_utils



class BeamImplTest(tft_unit.TransformTestCase):
    def setUp(self):
        tf.compat.v1.logging.info(
                'Starting test case: %s', self._testMethodName)

        self._context = beam_impl.Context(use_deep_copy_optimization=True)
        self._context.__enter__()

    def tearDown(self):
        self._context.__exit__()

    def _SkipIfExternalEnvironmentAnd(self, predicate, reason):
        if predicate and tft_unit.is_external_environment():
            raise unittest.SkipTest(reason)

    def testBasicType(self):
        config = {
                'timesteps': 3,
                'time_features': [],
                'features': ['a'],
                'enable_timestamp_features': False
        }

        input_data = [{'a': [1000.0, 2000.0, 3000.0]}]
        input_metadata = tft_unit.metadata_from_feature_spec(
                {'a': tf.io.VarLenFeature(tf.float32)})

        output = [[1000], [2000], [3000]]

        output = stats.zscore(output)

        expected_data = [{'Float32': output, 'LABEL': output}]

        expected_metadata = tft_unit.metadata_from_feature_spec({
                'Float32': tf.io.FixedLenFeature([config['timesteps'], 1],
                                                 tf.float32),
                'LABEL': tf.io.FixedLenFeature([config['timesteps'], 1],
                                               tf.float32)
        })

        preprocessing_fn = functools.partial(
                encoder_decoder_preprocessing.preprocessing_fn,
                custom_config=config)

        self.assertAnalyzeAndTransformResults(
                input_data,
                input_metadata,
                preprocessing_fn,
                expected_data,
                expected_metadata)

    def testMixedType(self):
        config = {
                'timesteps': 3,
                'time_features': ['MINUTE', 'MONTH', 'HOUR', 'DAY', 'YEAR'],
                'features': ['a', 'b'],
                'enable_timestamp_features': False
        }

        input_data = [{
                'a': [1000.0, 2000.0, 3000.0], 'b': [3000, 2000, 1000]
        }]
        input_metadata = tft_unit.metadata_from_feature_spec({
                'a': tf.io.VarLenFeature(tf.float32),
                'b': tf.io.VarLenFeature(tf.int64)
        })

        output = [[1000.0, 3000.0], [2000.0, 2000.0], [3000.0, 1000.0]]

        output = stats.zscore(output)

        expected_data = [{'Float32': output, 'LABEL': output}]

        expected_metadata = tft_unit.metadata_from_feature_spec({
                'Float32': tf.io.FixedLenFeature([config['timesteps'], 2],
                                                 tf.float32),
                'LABEL': tf.io.FixedLenFeature([config['timesteps'], 2],
                                               tf.float32)
        })

        preprocessing_fn = functools.partial(
                encoder_decoder_preprocessing.preprocessing_fn,
                custom_config=config)

        self.assertAnalyzeAndTransformResults(
                input_data,
                input_metadata,
                preprocessing_fn,
                expected_data,
                expected_metadata)

    def testWithTimeStamps(self):

        config = {
                'timesteps': 2,
                'time_features': ['MINUTE', 'MONTH', 'HOUR', 'DAY', 'YEAR'],
                'features': ['float32', 'foo_TIMESTAMP'],
                'enable_timestamp_features': True
        }

        # The values will need to be different enough for the zscore not to nan
        timestamp_1 = int(datetime(2000, 1, 1, 0, 0, 0).timestamp())
        timestamp_2 = int(datetime(2001, 6, 15, 12, 30, 30).timestamp())

        input_data = [{
                'float32': [1000.0, 2000.0],
                'foo_TIMESTAMP': [timestamp_1 * 1000, timestamp_2 * 1000]
        }]
        input_metadata = tft_unit.metadata_from_feature_spec({
                'float32': tf.io.VarLenFeature(tf.float32),
                'foo_TIMESTAMP': tf.io.VarLenFeature(tf.int64)
        })

        output_timestep_1 = self.create_transform_output(timestamp_1)

        output_timestep_2 = self.create_transform_output(timestamp_2)

        for i in range(len(output_timestep_1)):
            values = stats.zscore([output_timestep_1[i], output_timestep_2[i]])
            n = numpy.isnan(values)
            values[n] = 0.0
            output_timestep_1[i] = values[0]
            output_timestep_2[i] = values[1]

        values = stats.zscore([1000.0, 2000.0])

        output_timestep_1.insert(0, values[0])
        output_timestep_2.insert(0, values[1])

        output = [output_timestep_1, output_timestep_2]

        expected_data = [{'Float32': output, 'LABEL': output}]

        expected_metadata = tft_unit.metadata_from_feature_spec({
                'Float32': tf.io.FixedLenFeature([config['timesteps'], 11],
                                                 tf.float32),
                'LABEL': tf.io.FixedLenFeature([config['timesteps'], 11],
                                               tf.float32)
        })

        preprocessing_fn = functools.partial(
                encoder_decoder_preprocessing.preprocessing_fn,
                custom_config=config)

        self.assertAnalyzeAndTransformResults(
                input_data,
                input_metadata,
                preprocessing_fn,
                expected_data,
                expected_metadata)

    def create_transform_output(self, timestamp: int) -> [float]:
        # Needs to be in lexical order
        sin_day = math.sin(timestamp * (2.0 * math.pi / ts_utils.DAY))
        cos_day = math.cos(timestamp * (2.0 * math.pi / ts_utils.DAY))

        sin_hour = math.sin(timestamp * (2.0 * math.pi / ts_utils.HOUR))
        cos_hour = math.cos(timestamp * (2.0 * math.pi / ts_utils.HOUR))

        sin_min = math.sin(timestamp * (2.0 * math.pi / ts_utils.MINUTE))
        cos_min = math.cos(timestamp * (2.0 * math.pi / ts_utils.MINUTE))

        sin_month = math.sin(timestamp * (2.0 * math.pi / ts_utils.MONTH))
        cos_month = math.cos(timestamp * (2.0 * math.pi / ts_utils.MONTH))

        sin_year = math.sin(timestamp * (2.0 * math.pi / ts_utils.YEAR))
        cos_year = math.cos(timestamp * (2.0 * math.pi / ts_utils.YEAR))

        return [
                cos_day,
                cos_hour,
                cos_min,
                cos_month,
                cos_year,
                sin_day,
                sin_hour,
                sin_min,
                sin_month,
                sin_year
        ]


if __name__ == '__main__':
    tft_unit.main()
