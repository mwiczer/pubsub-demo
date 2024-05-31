// pubsub-demo is a demo attempting to implement a publisher-sucriber model using go concurrency.
package main

import (
	"context"
	"log"
	"time"
)

type pubSubRunner struct {
	// Arrows in the type annotation protect us from accidentally writing back into the Publisher
	Publisher   <-chan string
	Subscribers []chan<- string
}

func (ps pubSubRunner) Run(ctx context.Context) {
	i := 0
	for {
		var msg string
		// Receive from publisher
		select {
		case msg = <-ps.Publisher:
		case <-ctx.Done():
			log.Printf("Exiting fanout goroutine: %v", ctx.Err())
			return
		}
		i++
		// Fanout
		log.Printf("Forwarding message %d to subscribers...", i)
		for _, subCh := range ps.Subscribers {
			select {
			case subCh <- msg:
			case <-ctx.Done():
				log.Printf("Exiting fanout goroutine: %v", ctx.Err())
				return
			}
		}
	}
}

// RunPubSub creates a publisher channel which, when written to, fans the message out to all provided subscribers.
//
// It spawns a goroutine to manage the fanout. The publisher channel is unbuffered, therefore it is throttled by the slowest subscriber.
func RunPubSub(ctx context.Context, subscribers ...chan<- string) chan<- string {
	publisher := make(chan string)
	ps := pubSubRunner{
		Publisher:   publisher,
		Subscribers: subscribers,
	}
	go ps.Run(ctx)
	return publisher
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	subCh1 := make(chan string)
	subCh2 := make(chan string)
	pubCh := RunPubSub(ctx, subCh1, subCh2)

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
