// pubsub-demo is a demo attempting to implement a publisher-sucriber model using go concurrency.
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/mwiczer/pubsub-demo/pubsub"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	subCh1 := make(chan string)
	subCh2 := make(chan string)
	pubCh, doneCh := pubsub.RunPubSub(ctx, subCh1, subCh2)

	// Use the WaitGroup so make sure we don't exit the binary until the subscribers have successfully processed any context cancellation signal.
	wg := sync.WaitGroup{}

	// Listen for messages
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case msg := <-subCh1:
				log.Printf("Received message on sub1: %q", msg)
			case <-ctx.Done():
				log.Printf("Exiting listener 1: %v", ctx.Err())
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	}(ctx)

	// Listen for messages
	wg.Add(1)
	go func(ctx context.Context) {
		defer wg.Done()
		for {
			select {
			case msg := <-subCh2:
				log.Printf("Received message on sub2: %q", msg)
			case <-ctx.Done():
				log.Printf("Exiting listener 2: %v", ctx.Err())
				return
			}
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
		case pubCh <- msg:
		case <-ctx.Done():
			log.Printf("Canceling channel write: %v", ctx.Err())
			break PublishLoop
		}
	}

	// In order to wait long enough to properly process all messages:
	// 1. Close the publish channel. This signals the publisher to return as long as it's not still in the middle of its fanout loop.
	// 2. Read from the PubSub done channel. This is how we know that no fanout loops are ongoing.
	// 3. Cancel the context. This causes the listeners to return.
	// 4. Wait for the WaitGroup, which signals that the listeners have processed any messages they are going to process.
	// Phew, that's an involved dance! Is there a better way, besides the router style?
	close(pubCh)
	<-doneCh
	cancel()
	wg.Wait()
}
