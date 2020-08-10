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

import absl
import tensorflow as tf
import tensorflow_transform as tft

from tensorflow import keras
from tensorflow.keras.models import Model


def _build_keras_model(
        tf_transform_output: tft.TFTransformOutput,
        timesteps: int,
        number_features: int,
        outer_units: int = 32,
        inner_units: int = 16,
) -> tf.keras.Model:
    """Creates a Dn LSTM Keras model
    Returns:
      A Keras Model for auto encode-decode of timeseries data.
    """

    print(
            f'timesteps = {timesteps}, number_features = {number_features}, '
            f' outer_units = {outer_units}, inner_units = {inner_units}')

    visible = keras.layers.Input(shape=(timesteps, number_features))

    hidden = keras.layers.LSTM(
            outer_units,
            activation='selu',
            recurrent_dropout=0.05,
            return_sequences=True,
            kernel_initializer='he_uniform')(
                    visible)
    hidden = keras.layers.BatchNormalization()(hidden)

    hidden = keras.layers.LSTM(
            inner_units,
            activation='selu',
            return_sequences=False,
            kernel_initializer='he_uniform')(
                    hidden)
    hidden = keras.layers.BatchNormalization()(hidden)

    hidden = keras.layers.RepeatVector(timesteps)(hidden)

    hidden = keras.layers.LSTM(
            inner_units,
            activation='selu',
            return_sequences=True,
            kernel_initializer='he_uniform')(
                    hidden)
    hidden = keras.layers.BatchNormalization()(hidden)

    hidden = keras.layers.LSTM(
            outer_units,
            activation='selu',
            return_sequences=True,
            kernel_initializer='he_uniform')(
                    hidden)
    hidden = keras.layers.BatchNormalization()(hidden)

    output = keras.layers.TimeDistributed(keras.layers.Dense(number_features))(
            hidden)

    model = Model(inputs=visible, outputs=output)

    optimizer = keras.optimizers.Adam()

    model.compile(
            metrics=[
                    tf.keras.metrics.MeanAbsoluteError(
                            name='mean_absolute_error')
            ],
            loss='mean_squared_error',
            optimizer=optimizer)

    model.summary(print_fn=absl.logging.info)

    return model
