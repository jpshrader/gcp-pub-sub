package main

import (
	"context"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

func main() {
	ctx := context.Background()
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	client, err := pubsub.NewClient(ctx, "gcp-pub-sub")
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	defer client.Close()

	topics := client.Topics(ctx)
	for {
		topic, err := topics.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("failed to get topic: %v", err)
		}
		topic.Delete(ctx)
		log.Printf("deleted topic: %v", topic)
	}

	subs := client.Subscriptions(ctx)
	for {
		sub, err := subs.Next()
		if err != nil {
			if err == iterator.Done {
				break
			}
			log.Fatalf("failed to get subscription: %v", err)
		}
		sub.Delete(ctx)
		log.Printf("deleted subscription: %v", sub)
	}

	log.Println("deleted all topics and subscriptions")
}
