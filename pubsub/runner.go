// Package pubsub extracts the pubsub demo into a library.
package pubsub

import (
	"context"
	"log"
)

type pubSubRunner[T any] struct {
	// Arrows in the type annotation protect us from accidentally writing back into the Publisher
	Publisher   <-chan T
	Subscribers []chan<- T
}

func (ps pubSubRunner[T]) Run(ctx context.Context) {
	i := 0
	for {
		var msg T
		// Receive from publisher
		select {
		case m, ok := <-ps.Publisher:
			if !ok {
				log.Printf("Exiting fanout on pubsub closure")
				// We could loop throush the subscribers here and call close(subCh) here,
				// but I don't think that's safe, since closing a channel after write causes a panic.
				// The subscriber channels are provided to RunPubSub by the client,
				// so we can't prevent the client from attempting to write to the channel after write.
				return
			}
			msg = m
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
// In addition to the data channel, it returns a done channel, which signals when the runner has closed.
// It spawns a goroutine to manage the fanout. The publisher channel is unbuffered, therefore it is throttled by the slowest subscriber.
func RunPubSub[T any](ctx context.Context, subscribers ...chan<- T) (chan<- T, <-chan struct{}) {
	publisher := make(chan T)
	ps := pubSubRunner[T]{
		Publisher:   publisher,
		Subscribers: subscribers,
	}
	done := make(chan struct{}, 1)
	go func() {
		ps.Run(ctx)
		done <- struct{}{}
	}()
	return publisher, done
}
