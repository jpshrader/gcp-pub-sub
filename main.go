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

const (
    // Project/Topic settings
    projectId         = "gcp-pub-sub"
    topicId           = "topicId"
    deadLetterTopicId = "deadletter-topicId"

    // Subscription settings
    subscriptionId         = "subscriptionId"
    ackDeadline            = 15 * time.Second
    maxDeliveryAttempts    = 10
    minBackoff             = 1 * time.Second
    maxBackoff             = 10 * time.Second
    maxOutstandingMessages = 10
    numGoroutines          = 2
    synchronous            = true

    // Settings
    numMessagesToPublish      = 2
    timeBetweenMessagePublish = 0 * time.Second
    timeToProcessMessage      = 2 * time.Second
)

func createMessage(messageNum int) pubsub.Message {
    return pubsub.Message{
        Data: []byte(strconv.Itoa(messageNum)),
    }
}

func shouldAckMessage(msg *pubsub.Message) bool {
    messageCount, err := strconv.ParseInt(string(msg.Data), 10, 64)
    if err != nil {
        return false
    }

    if messageCount%2 == 0 {
        return false
    }
    return true
}

func main() {
    ctx := context.Background()
    os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

    client, err := pubsub.NewClient(ctx, projectId)
    if err != nil {
        log.Fatalf("failed to create client: %v", err)
    }
    defer client.Close()

    topic, err := createTopic(ctx, client, topicId)
    if err != nil {
        log.Fatalf("failed to create topic: %v", err)
    }
    deadletterTopic, err := createTopic(ctx, client, deadLetterTopicId)
    if err != nil {
        log.Fatalf("failed to create deadletter topic: %v", err)
    }

    subscription, err := createSubscription(ctx, client, topic, deadletterTopic)
    if err != nil {
        log.Fatalf("failed to create subscription: %v", err)
    }

    var wg sync.WaitGroup
    wg.Add(1)
    func() {
        for i := 0; i < numMessagesToPublish; i++ {
            if timeBetweenMessagePublish > time.Duration(0) {
                time.Sleep(timeBetweenMessagePublish)
            }
            err := publishMessage(ctx, topic, createMessage(i))
            if err != nil {
                log.Fatalf("failed to publish message: %v", err)
            }
        }
        log.Println("published all messages")
    }()

    wg.Add(1)
    func() {
        receiveMessages(ctx, subscription)
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
        log.Printf("created topic: %v", topic)
    }

    return topic, nil
}

func createSubscription(ctx context.Context, client *pubsub.Client, topic *pubsub.Topic, dlTopic *pubsub.Topic) (*pubsub.Subscription, error) {
    subscription := client.Subscription(subscriptionId)

    exists, err := subscription.Exists(ctx)
    if err != nil {
        return nil, err
    }

    if !exists {
        cfg := pubsub.SubscriptionConfig{
            Topic:       topic,
            AckDeadline: ackDeadline,
            RetryPolicy: &pubsub.RetryPolicy{
                MinimumBackoff: minBackoff,
                MaximumBackoff: maxBackoff,
            },
            DeadLetterPolicy: &pubsub.DeadLetterPolicy{
                DeadLetterTopic:     dlTopic.String(),
                MaxDeliveryAttempts: maxDeliveryAttempts,
            },
        }
        subscription, err = client.CreateSubscription(ctx, subscriptionId, cfg)
        if err != nil {
            return nil, err
        }
        subscription.ReceiveSettings = pubsub.ReceiveSettings{
            MaxExtension:           ackDeadline,
            MaxOutstandingMessages: maxOutstandingMessages,
            NumGoroutines:          numGoroutines,
            Synchronous:            synchronous,
        }
        log.Printf("created subscription: %v", subscription)
    }

    return subscription, nil
}

func publishMessage(ctx context.Context, topic *pubsub.Topic, message pubsub.Message) error {
    result := topic.Publish(ctx, &message)
    _, err := result.Get(ctx)
    if err != nil {
        return err
    }
    return err
}

func receiveMessages(ctx context.Context, subscription *pubsub.Subscription) {
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

        if !shouldAckMessage(msg) {
            log.Printf("message: %d, attempt #%d ❌\n", messageCount, deliveryAttempt)
            msg.Nack()
            return
        }

        time.Sleep(timeToProcessMessage)
        msg.Ack()
        log.Printf("message: %d, attempt #%d ✅\n", messageCount, deliveryAttempt)
    })
    if err != nil {
        log.Printf("error receiving messages: %v", err)
    }
}
