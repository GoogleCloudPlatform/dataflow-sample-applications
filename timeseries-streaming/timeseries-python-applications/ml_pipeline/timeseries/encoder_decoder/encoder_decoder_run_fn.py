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

from typing import Text
import tensorflow as tf
import tensorflow_transform as tft
import timeseries.encoder_decoder.encoder_decoder_model as encoder_decoder_model

from tfx.components.trainer.executor import TrainerFnArgs


def _input_fn(
        file_pattern: Text,
        tf_transform_output: tft.TFTransformOutput,
        batch_size: int = 2) -> tf.data.Dataset:
    transformed_feature_spec = (
            tf_transform_output.transformed_feature_spec().copy())

    dataset = tf.data.experimental.make_batched_features_dataset(
            file_pattern=file_pattern,
            batch_size=batch_size,
            # Shuffle must be False, as Zip operation is not transitive
            shuffle=False,
            features=transformed_feature_spec,
            reader=_gzip_reader_fn)

    train_dataset = dataset.map(create_training_data)
    label_dataset = dataset.map(create_label_data)
    dataset = tf.data.Dataset.zip((train_dataset, label_dataset))

    return dataset


def create_training_data(features):
    return features['Float32']


def create_label_data(features):
    return features['LABEL']


def _gzip_reader_fn(filenames):
    """Small utility returning a record reader that can read gzip'ed files."""
    return tf.data.TFRecordDataset(filenames, compression_type="GZIP")


def _get_serve_tf_examples_fn(model, tf_transform_output):
    """Returns a function that parses a serialized tf.Example."""

    model.tft_layer = tf_transform_output.transform_features_layer()

    @tf.function
    def serve_tf_examples_fn(serialized_tf_examples):
        """Returns the output to be used in the serving signature."""
        feature_spec = tf_transform_output.raw_feature_spec()
        parsed_features = tf.io.parse_example(
                serialized_tf_examples, feature_spec)
        transformed_features = create_training_data(
                model.tft_layer(parsed_features))
        return model(transformed_features)

    return serve_tf_examples_fn


def run_fn(fn_args: TrainerFnArgs):
    """Train the model based on given args.
    Args:
      fn_args: Holds args used to train the model as name/value pairs.
    """
    tf_transform_output = tft.TFTransformOutput(fn_args.transform_output)

    print(f"Parameters {fn_args}")

    train_dataset = _input_fn(
            fn_args.train_files,
            tf_transform_output,
            batch_size=fn_args.train_batches)

    eval_dataset = _input_fn(
            fn_args.eval_files,
            tf_transform_output,
            batch_size=fn_args.eval_batches)

    # mirrored_strategy = tf.distribute.MirroredStrategy()
    # with mirrored_strategy.scope():
    model = encoder_decoder_model.build_keras_model(
            timesteps=fn_args.timesteps,
            number_features=fn_args.number_features,
            outer_units=fn_args.outer_units,
            inner_units=fn_args.inner_units)

    steps_per_epoch = fn_args.training_example_count / fn_args.train_batches

    tensorboard_callback = tf.keras.callbacks.TensorBoard()

    model.fit(
            train_dataset,
            epochs=int(fn_args.train_steps / steps_per_epoch),
            steps_per_epoch=steps_per_epoch,
            validation_data=eval_dataset,
            validation_steps=fn_args.eval_steps,
            callbacks=[tensorboard_callback])

    signatures = {
            'serving_default': _get_serve_tf_examples_fn(
                    model, tf_transform_output).get_concrete_function(
                            tf.TensorSpec(
                                    shape=[None],
                                    dtype=tf.string,
                                    name='examples')),
    }

    model.save(
            fn_args.serving_model_dir, save_format='tf', signatures=signatures)
