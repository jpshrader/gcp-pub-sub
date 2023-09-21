package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

type pubSubConfig struct {
	ProjectId           string
	TopicId             string
	DeadLetterTopicId   string
	SubscriptionId      string
	AckDeadline         time.Duration
	ReceiveSettings     pubsub.ReceiveSettings
	NumMessages         int
	TimeBetweenMessages time.Duration
	MaxDeliveryAttempts int
	RetryPolicy         pubsub.RetryPolicy
}

func main() {
	config := pubSubConfig{
		ProjectId:           "gcp-pub-sub",
		TopicId:             "topicId",
		DeadLetterTopicId:   "deadletter-topicId",
		SubscriptionId:      "subscriptionId",
		AckDeadline:         15 * time.Second,
		NumMessages:         2,
		TimeBetweenMessages: 2 * time.Second,
		MaxDeliveryAttempts: 10,
		ReceiveSettings: pubsub.ReceiveSettings{
			MaxOutstandingMessages: 0,
			NumGoroutines:          1,
			Synchronous:            true,
			MaxExtension:           10 * time.Second,
		},
		RetryPolicy: pubsub.RetryPolicy{
			MinimumBackoff: 3 * time.Second,
			MaximumBackoff: 10 * time.Second,
		},
	}

	ctx := context.Background()
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	client, err := pubsub.NewClient(ctx, config.ProjectId)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	topic, err := createTopic(ctx, client, config.TopicId)
	if err != nil {
		log.Fatalf("failed to create topic: %v", err)
	}
	deadletterTopic, err := createTopic(ctx, client, config.DeadLetterTopicId)
	if err != nil {
		log.Fatalf("failed to create deadletter topic: %v", err)
	}

	subscription, err := createSubscription(ctx, client, topic, deadletterTopic, config)
	if err != nil {
		log.Fatalf("failed to create subscription: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for i := 0; i < config.NumMessages; i++ {
			message := strconv.Itoa(i)
			err := publishMessage(ctx, topic, message)
			if err != nil {
				log.Fatalf("failed to publish message: %v", err)
			}
		}
		log.Println("published all messages")
	}()

	wg.Add(1)
	go func() {
		receiveMessages(ctx, subscription, config)
	}()

	wg.Wait()
	log.Println("done")
}

func createTopic(ctx context.Context, client *pubsub.Client, topicID string) (*pubsub.Topic, error) {
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

func createSubscription(ctx context.Context, client *pubsub.Client, topic *pubsub.Topic, dlTopic *pubsub.Topic, config pubSubConfig) (*pubsub.Subscription, error) {
	subscription := client.Subscription(config.SubscriptionId)

	exists, err := subscription.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !exists {
		cfg := pubsub.SubscriptionConfig{
			Topic:       topic,
			AckDeadline: config.AckDeadline,
			RetryPolicy: &config.RetryPolicy,
			DeadLetterPolicy: &pubsub.DeadLetterPolicy{
				DeadLetterTopic:     dlTopic.String(),
				MaxDeliveryAttempts: config.MaxDeliveryAttempts,
			},
		}
		subscription, err = client.CreateSubscription(ctx, config.SubscriptionId, cfg)
		if err != nil {
			return nil, err
		}
		subscription.ReceiveSettings = config.ReceiveSettings
	}

	return subscription, nil
}

func publishMessage(ctx context.Context, topic *pubsub.Topic, message string) error {
	result := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(message),
	})
	_, err := result.Get(ctx)
	if err != nil {
		return err
	}
	return err
}

func receiveMessages(ctx context.Context, subscription *pubsub.Subscription, config pubSubConfig) {
	err := subscription.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		messageCount, err := strconv.ParseInt(string(msg.Data), 10, 64)
		if err != nil {
			log.Printf("error parsing message: %v", err)
			msg.Ack()
			return
		}
		deliveryAttempt := 1
		if msg.DeliveryAttempt != nil {
			deliveryAttempt = *msg.DeliveryAttempt
		}

		if messageCount%2 == 0 {
			log.Printf("message: %d, attempt #%d ❌\n", messageCount, deliveryAttempt)
			msg.Nack()
			return
		}

		log.Printf("message: %d, attempt #%d ✅\n", messageCount, deliveryAttempt)
		time.Sleep(config.TimeBetweenMessages)
		msg.Ack()
	})
	if err != nil {
		log.Printf("error receiving messages: %v", err)
	}
}
