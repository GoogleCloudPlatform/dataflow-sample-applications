import datetime
import tempfile
import os

import tensorflow as tf
import unittest

from absl.testing import absltest
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from tfx.dsl.io import fileio
from tfx.proto import example_gen_pb2
from tfx.types import standard_component_specs, standard_artifacts, artifact_utils
from tfx.utils import proto_utils

import ml_pipeline.timeseries.timeseries_example_gen.executor as executor

import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to

class ExecutorTest(absltest.TestCase):

    def _bytes_feature(self,value):
        """Returns a bytes_list from a string / byte."""
        if isinstance(value, type(tf.constant(0))):
            value = value.numpy()  # BytesList won't unpack a string from an EagerTensor.
        return tf.train.Feature(bytes_list=tf.train.BytesList(value=[value]))


    def _float_feature(self,value):
        """Returns a float_list from a float / double."""
        return tf.train.Feature(float_list=tf.train.FloatList(value=[value]))


    def _int64_feature(self,value):
        """Returns an int64_list from a bool / enum / int / uint."""
        return tf.train.Feature(int64_list=tf.train.Int64List(value=[value]))


    def _serialize_example(self, start_time, end_time, feature):
        """
        Creates a tf.train.Example message ready to be written to a file.
        """
        # Create a dictionary mapping the feature name to the tf.train.Example-compatible
        # data type.
        feature = {
            'first_timestamp': self._int64_feature(start_time),
            'last_timestamp': self._int64_feature(end_time),
            'feature': self._int64_feature(feature),
        }

        # Create a Features message using tf.train.Example.

        example_proto = tf.train.Example(features=tf.train.Features(feature=feature))
        return example_proto.SerializeToString()


    def createFile(self) -> str:
        self.setUp()
        times = [self.time_start + i * 1000 for i in range(0, 5)]

        examples = [self._serialize_example(i, i, i) for i in times]
        filename = f'{os.path.dirname(__file__)}/testdata/data.tr'
        with tf.io.TFRecordWriter(filename) as writer:
            for i in examples:
                writer.write(i)
        return filename

    def setUp(self):
        super().setUp()
        self._input_data_dir = f'{os.path.dirname(__file__)}/testdata/data.tr'
        self.time_start = tf.cast(datetime.datetime.fromisoformat('2000-01-01T00:00:00+00:00').timestamp() * 1000, tf.int64)


    def test_min_max_value_extraction(self):
        """
        Test that given a set of TF.Example files the min value is correctly pulled out
        """

        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            examples, min_max = (p | beam.io.ReadFromTFRecord(self._input_data_dir)
                                 | beam.Map(lambda i : tf.train.Example.FromString(i))
                                 | executor.ProcessTimeseriesExamples())

            # Strange behaviour with value remaining as tensor even though it has been extracted
            assert_that(min_max, equal_to([{'min': self.time_start.numpy(), 'max': self.time_start.numpy() + 4000}]))


    def test_slice_length_too_short_err(self):
        executor.slice_time(min_max={'min': 1000, 'max': 5000})


    def test_slice_defaults(self):
        result = executor.slice_time(min_max={'min': 0, 'max': 5000})
        assert result == [3000, 4000, 5000], f'Value was {result}'

    def test_slice_ms_length(self):
        result = executor.slice_time(min_max={'min': 0, 'max': 5})
        assert result == [3, 4, 5], f'Value was {result}'

    def test_slice_min_offset(self):
        result = executor.slice_time(min_max={'min': 5, 'max': 10})
        assert result == [8, 9, 10], f'Value was {result}'

    def test_correct_train_eval_buckets(self):
        """
        Given TF.Example files with time span of 0 to 4, examples [0,3] should be marked as train and [4,5] marked as eval
        """


    def test_tfexample_without_lasttime(self):
        """
        Given TF.Example files that do not have lasttime files with time span of 0 to 4, examples [0,3] should be marked as
        train and [4,5] marked as eval
        """


    def test_deadletter_stradle_data(self):
        """
        Start and end times that straddle a separation line, ensure they are sent to deadletter que
        """


    def test_run_statistics_recorded(test):
        """
        Ensure metrics for the straddle are correct given span of 0 to 4
        """


    def test_dense_output_check(self):
        """
        The spans of data being output should have continuous data throughout the timeline. Throw error when this is not
        true.
        """


    def test_tf_example_to_temporal_split(self):
        """
        Test that given a set of TF.Example files the min value is correctly pulled out
        """

        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            examples, min_max = (p | beam.io.ReadFromTFRecord(self._input_data_dir)
                                 | beam.Map(lambda i : tf.train.Example.FromString(i))
                                 | executor.ProcessTimeseriesExamples())
            # Test that the output has been correctly split and stored

            time = self.time_start.numpy()

            # Test for training set
            split_0 = examples['0'] | beam.Map(lambda x: x[1].features.feature['feature'].int64_list.value[0])
            assert_that(split_0, equal_to([time, time+1000, time+2000]), label='split_0')

            # Test for training set
            split_1 = examples['1'] | beam.Map(lambda x: x[1].features.feature['feature'].int64_list.value[0])
            assert_that(split_1, equal_to([time+3000]), label='split_1')

            # Test for training set
            split_2 = examples['2'] | beam.Map(lambda x: x[1].features.feature['feature'].int64_list.value[0])
            assert_that(split_2, equal_to([time+4000]), label='split_2')

    def test_tf_sequence_example_to_temporal_split(self):
        print()


    def testDo(self):
        output_data_dir = os.path.join(
            os.environ.get('TEST_UNDECLARED_OUTPUTS_DIR', self.create_tempdir()),
            self._testMethodName)

        # Create output dict.
        examples = standard_artifacts.Examples()
        examples.uri = output_data_dir
        output_dict = {standard_component_specs.EXAMPLES_KEY: [examples]}

        # Create exec properties.
        exec_properties = {
            standard_component_specs.INPUT_BASE_KEY:
                self._input_data_dir,
            standard_component_specs.INPUT_CONFIG_KEY:
                proto_utils.proto_to_json(
                    example_gen_pb2.Input(splits=[
                        example_gen_pb2.Input.Split(name=self._input_data_dir, pattern=self._input_data_dir),
                    ])),
            standard_component_specs.OUTPUT_DATA_FORMAT_KEY:
                example_gen_pb2.PayloadFormat.FORMAT_TF_EXAMPLE,
            standard_component_specs.OUTPUT_CONFIG_KEY:
                proto_utils.proto_to_json(
                    example_gen_pb2.Output(
                        split_config=example_gen_pb2.SplitConfig(splits=[
                            example_gen_pb2.SplitConfig.Split(
                                name='train', hash_buckets=3),
                            example_gen_pb2.SplitConfig.Split(
                                name='validate', hash_buckets=2),
                            example_gen_pb2.SplitConfig.Split(
                                name='test', hash_buckets=1)
                        ])))
        }

        timeseries_example_gen = executor.Executor()
        timeseries_example_gen.Do({}, output_dict, exec_properties)

        self.assertEqual(
            artifact_utils.encode_split_names(['train', 'validate', 'test']),
            examples.split_names)

        # # Check CSV example gen outputs.
        train_output_file = os.path.join(examples.uri, 'Split-train',
                                         'data_tfrecord-00000-of-00001.gz')
        validate_output_file = os.path.join(examples.uri, 'Split-validate',
                                        'data_tfrecord-00000-of-00001.gz')
        test_output_file = os.path.join(examples.uri, 'Split-test',
                                        'data_tfrecord-00000-of-00001.gz')
        self.assertTrue(fileio.exists(train_output_file))
        self.assertTrue(fileio.exists(validate_output_file))
        self.assertTrue(fileio.exists(test_output_file))
        self.assertGreater(
            fileio.open(train_output_file).size(),
            fileio.open(validate_output_file).size())
        self.assertGreater(
            fileio.open(train_output_file).size(),
            fileio.open(test_output_file).size())

if __name__ == '__main__':
    absltest.main()