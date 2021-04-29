/**
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package test

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/gruntwork-io/terratest/modules/gcp"
	"github.com/gruntwork-io/terratest/modules/terraform"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestPubsub(t *testing.T) {

	projectID := gcp.GetGoogleProjectIDFromEnvVar(t)
	opts := &terraform.Options{
		TerraformDir:       "../../examples/pubsub",
		Vars:               map[string]interface{}{"project_id": projectID},
		MaxRetries:         1,
		TimeBetweenRetries: 5 * time.Second,
	}
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	terraform.Init(t, opts)
	terraform.Apply(t, opts)

	testCases := []struct {
		name      string
		topicName string
		subName   string
		message   string
		expected  string
	}{
		{
			"ClickstreamInbound",
			terraform.Output(t, opts, "clickstream_inbound_topic"),
			terraform.Output(t, opts, "clickstream_inbound_sub"),
			"Clickstream-inbound-test-message",
			"Clickstream-inbound-test-message",
		},
		{
			"InventoryInbound",
			terraform.Output(t, opts, "inventory_inbound_topic"),
			terraform.Output(t, opts, "inventory_inbound_sub"),
			"Inventory-inbound-test-message",
			"Inventory-inbound-test-message",
		},
		{
			"TransactionsInbound",
			terraform.Output(t, opts, "transactions_inbound_topic"),
			terraform.Output(t, opts, "transactions_inbound_sub"),
			"Transactions-inbound-test-message",
			"Transactions-inbound-test-message",
		},
	}

	t.Run("group", func(t *testing.T) {
		for _, testCase := range testCases {

			testCase := testCase

			t.Run(testCase.name, func(t *testing.T) {
				t.Parallel()

				publishMessage(ctx, client, testCase.topicName, testCase.message)
				actual := receiveMessage(ctx, client, t, testCase.subName)

				assert.Equal(t, testCase.expected, actual)
			})
		}
	})
	_, err = terraform.DestroyE(t, opts)
	if err != nil {
		t.Fatalf("Please refer to the automated test README file about how to manually delete resources in order to keep the test environment clean.", err)
	}
}

func publishMessage(ctx context.Context, client *pubsub.Client, topicName string, expected string) {

	newTopic := client.Topic(topicName)

	defer newTopic.Stop()

	newTopic.Publish(ctx, &pubsub.Message{Data: []byte(expected)})
}

func receiveMessage(ctx context.Context, client *pubsub.Client, t *testing.T, subName string) string {

	newSub := client.Subscription(subName)
	cctx, cancel := context.WithCancel(ctx)
	var actualByte []byte

	err := newSub.Receive(cctx, func(ctx context.Context, m *pubsub.Message) {
		defer cancel()
		actualByte = m.Data
		m.Ack()
	})
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}

	return string(actualByte)
}
