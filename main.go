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

	pubCh := make(chan string)
	subCh1 := make(chan string)
	subCh2 := make(chan string)
	subscribers := []chan string{subCh1, subCh2}

	// Fanout the messages:
	go func(ctx context.Context) {
		i := 0
		for {
			var msg string
			select {
			case msg = <-pubCh:
			case <-ctx.Done():
				log.Printf("Exiting fanout goroutine: %v", ctx.Err())
				return
			}
			i++
			log.Printf("Forwarding message %d to subscribers...", i)
			for _, subCh := range subscribers {
				select {
				case subCh <- msg:
				case <-ctx.Done():
					log.Printf("Exiting fanout goroutine: %v", ctx.Err())
					return
				}
			}
		}
	}(ctx)

	// Listen for messages
	go func(ctx context.Context) {
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
	go func(ctx context.Context) {
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
	// Extra sleep to let the listener receive the last message.
	time.Sleep(500 * time.Millisecond)
}
