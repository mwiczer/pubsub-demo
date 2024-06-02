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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := &pubsub.Router{}

	// Use the WaitGroup so make sure we don't exit the binary until the subscribers have successfully processed any context cancellation signal.
	wg := sync.WaitGroup{}

	// Listen for messages
	wg.Add(1)
	ps.Subscribe(func(ch <-chan string) {
		defer wg.Done()
		for {
			select {
			case msg := <-ch:
				log.Printf("Received message on sub1: %q", msg)
			case <-ctx.Done():
				log.Printf("Exiting listener 1: %v", ctx.Err())
				return
			}
			time.Sleep(500 * time.Millisecond)
		}
	})

	// Listen for messages
	wg.Add(1)
	ps.Subscribe(func(ch <-chan string) {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		select {
		case msg := <-ch:
			log.Printf("Received message on sub2: %q", msg)
		case <-ctx.Done():
			log.Printf("Exiting listener 2: %v", ctx.Err())
			return
		}
		log.Printf("Spawning nested listener")
		wg.Add(1)
		ps.Subscribe(func(ch <-chan string) {
			wg.Done()
			time.Sleep(100 * time.Millisecond)
			select {
			case msg := <-ch:
				log.Printf("Received message on nested subscriber: %q", msg)
			case <-ctx.Done():
				log.Printf("Exiting nested listener: %v", ctx.Err())
				return
			}
		})
		// Return after the first message is received.
		// This no longer breaks the other subscriber,
		log.Printf("Exiting listener 2.")
	})

	// Publish the messages
	messages := []string{
		"foo", "bar", "baz", "abc", "def",
	}
	for i, msg := range messages {
		log.Printf("Sending message %d...", i)
		// Unlike with the runner implementation, ps.Publish blocks until all (unbuffered) fanout channels have been written to.
		if err := ps.Publish(ctx, msg); err != nil {
			log.Printf("Canceling publisher loop: %v", err)
			break
		}
	}

	// In order to wait long enough to properly process all messages:
	// 1. Cancel the context.
	// 2. Wait for the subscribers to process that cancelled context. This is doable with wg.Wait()
	// Much easier than with the runner style. This is because ps.Publish blocks until fanout is complete.
	cancel()
	wg.Wait()
}
