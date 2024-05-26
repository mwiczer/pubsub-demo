// pubsub-demo is a demo attempting to implement a publisher-sucriber model using go concurrency.
package main

import (
	"context"
	"log"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ch := make(chan string)

	// Listen for messages
	go func(ctx context.Context) {
		for {
			select {
			case msg := <-ch:
				log.Printf("Received message: %q", msg)
			case <-ctx.Done():
				log.Printf("Exiting listener: %v", ctx.Err())
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}(ctx)

	// Publish the messages
	messages := []string{
		"foo", "bar", "baz", "abc", "def",
	}
PublishLoop:
	for i, msg := range messages {
		log.Printf("Sending message %d...", i)
		select {
		// Channel writing can be a branch of a select as well as channel reading.
		case ch <- msg:
		case <-ctx.Done():
			log.Printf("Canceling channel write: %v", ctx.Err())
			break PublishLoop
		}
	}
	// Extra sleep to let the listener receive the last message.
	time.Sleep(500 * time.Millisecond)
}
