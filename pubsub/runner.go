// Package pubsub extracts the pubsub demo into a library.
package pubsub

import (
	"context"
	"log"
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
