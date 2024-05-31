package pubsub

import (
	"context"
	"log"
)

// Router provides pubsub behavior by exposing Publish and Subscribe methods.
//
// It differs from the runner style in a few ways:
// 1. There's no "runner" goroutine. One goroutine per subscriber, but no extra one.
// 2. The relevant channels are made by the Router struct. In contrast, the consumer provdides subscriber channels for the runner.
// 3. Publishing blocks until fanout is complete.
//
// For now, the subscribers list must be static, but I think this architecture gives us a good opportunity to fix that.
type Router struct {
	subscribers []chan<- string
}

// Subscribe registers the provided listener as a subscriber to future published messages.
//
// Subscribe spawns a goroutine that lives as long as the listener.
func (ps *Router) Subscribe(listener func(<-chan string)) {
	subCh := make(chan string)
	ps.subscribers = append(ps.subscribers, subCh)

	go listener(subCh)
	log.Printf("Added subscriber #%d", len(ps.subscribers))
}

// Publish sends the provided message to all active subscribers.
func (ps *Router) Publish(ctx context.Context, msg string) error {
	for _, sub := range ps.subscribers {
		select {
		case sub <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
