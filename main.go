package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
)

func main() {
	projectID := "gcp-sup-sub"
	emulatorHost := "localhost:8085"

	// Set the environment variable to use the Pub/Sub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", emulatorHost)

	// Create a client to interact with the Pub/Sub emulator
	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer client.Close()

	topicID := "test-topic"
	subscriptionID := "test-subscription"

	// Create a topic
	topic, err := createTopic(client, topicID)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}

	// Create a subscription with your desired settings
	subscription, err := createSubscription(client, subscriptionID, topic, 30*time.Second, 10)
	if err != nil {
		log.Fatalf("Failed to create subscription: %v", err)
	}

	// Publish a message to the topic
	message := "Hello, Pub/Sub!"
	publishResult := publishMessage(topic, message)
	fmt.Printf("Message published: %s\n", publishResult)

	// Receive and process messages from the subscription
	receiveMessages(subscription)
}

func createTopic(client *pubsub.Client, topicID string) (*pubsub.Topic, error) {
	ctx := context.Background()
	topic := client.Topic(topicID)

	exists, err := topic.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		topic, err = client.CreateTopic(ctx, topicID)
		if err != nil {
			return nil, err
		}
	}

	return topic, nil
}

func createSubscription(client *pubsub.Client, subscriptionID string, topic *pubsub.Topic, ackDeadline time.Duration, maxOutstandingMessages int) (*pubsub.Subscription, error) {
	ctx := context.Background()
	subscription := client.Subscription(subscriptionID)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		cfg := pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: ackDeadline,
		}
		subscription, err = client.CreateSubscription(ctx, subscriptionID, cfg)
		if err != nil {
			return nil, err
		}
		subscription.ReceiveSettings = pubsub.ReceiveSettings{
			MaxOutstandingMessages: maxOutstandingMessages,
			NumGoroutines:          1,
			Synchronous:            true,
		}
	}

	return subscription, nil
}

func publishMessage(topic *pubsub.Topic, message string) string {
	ctx := context.Background()
	result := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(message),
	})
	id, err := result.Get(ctx)
	if err != nil {
		return fmt.Sprintf("Failed to publish: %v", err)
	}
	return fmt.Sprintf("Published message with ID: %s", id)
}

func receiveMessages(subscription *pubsub.Subscription) {
	ctx := context.Background()
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		fmt.Printf("Received message: %s\n", string(msg.Data))
		msg.Ack()
	})
	if err != nil {
		log.Printf("Error receiving messages: %v", err)
	}
}
