// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const express = require('express');
const bodyParser = require('body-parser');
const app = express();

app.use(bodyParser.json());

app.get('/', (req, res) => {
  console.log('Hello world received a request.');

  const target = process.env.TARGET || 'World';
  res.send(`Hello ${target}!`);
});

app.post('/json', (req, res) => {
  const dataLayer = JSON.stringify(req.body)
  console.log(`proxy POST request received dataLayer: ${dataLayer}`)

  const {PubSub} = require('@google-cloud/pubsub');

  // Instantiates a client
  const pubsub = new PubSub();

  const {Buffer} = require('safe-buffer');

  // Set Pub/Sub topic name
  let topicName = 'ecommerce-events';

  // References an existing topic
  const topic = pubsub.topic(topicName);

  // Publishes the message as a string, 
  const dataBuffer = Buffer.from(dataLayer);

  // Add two custom attributes, origin and username, to the message
  const customAttributes = {
    origin: 'gtm-cloud-run',
    username: 'gcp-demo',
  };

  // Publishes a message to Pub/Sub
  return topic
    .publish(dataBuffer, customAttributes)
    .then(() => res.status(200).send(`{"message": "pubsub message sent: ${dataBuffer}"}`))
    .catch(err => {
      console.error(err);
      res.status(500).send(err);
      return Promise.reject(err);
   });

  // console.log(`received /post ${data}`)
  // res.send(`received /post ${data}`);
})

app.get('/pixel', (req, res) => {
  console.log('Send data to Pub/Sub');

  // [START functions_pubsub_setup]
  const {PubSub} = require('@google-cloud/pubsub');

  // Instantiates a client
  const pubsub = new PubSub();

  const {Buffer} = require('safe-buffer');

  // Set Pub/Sub topic name
  let topicName = 'retail-stream';

  // References an existing topic
  const topic = pubsub.topic(topicName);

  const message = {
    data: {
      // eventTimestamp: 'req.body.eventTimestamp',
      message: {
        foo: "bar",
        hello: "world",
        url: req.url,
        params: req.params,
        query: req.query
      }
    },
  };

  // // build pub/sub message
  // const pubSubMessage = req.body.message;

  // const target = process.env.TARGET || 'World';

  // const message = req.body

  // convert object to string
  const data = JSON.stringify(message)

  // Publishes the message as a string, 
  const dataBuffer = Buffer.from(data);

  // Add two custom attributes, origin and username, to the message
  const customAttributes = {
    origin: 'gtm-cloud-run',
    username: 'gcp-demo',
  };

  // Publishes a message to Pub/Sub
  return topic
    .publish(dataBuffer, customAttributes)
    .then(() => res.status(200).send(`{"message": "pubsub message sent: ${dataBuffer}"}`))
    .catch(err => {
      console.error(err);
      res.status(500).send(err);
      return Promise.reject(err);
   });

  // res.send(`Data sent ${dataBuffer}!`);
});

module.exports = app;
