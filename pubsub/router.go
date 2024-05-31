package pubsub

import (
	"context"
	"log"
	"sync"

	"github.com/google/uuid"
)

// Router provides pubsub behavior by exposing Publish and Subscribe methods.
//
// It differs from the runner style in a few ways:
// 1. There's no "runner" goroutine. One goroutine per subscriber, but no extra one.
// 2. The relevant channels are made by the Router struct. In contrast, the consumer provdides subscriber channels for the runner.
// 3. Publishing blocks until fanout is complete.
// 4. "Self-healing". If one subscriber exits, we can still publish to other active subscribers.
//   - This is still flaky, and not fully robust. We'll need to use something better than sync.Map for that.
type Router struct {
	subscribers sync.Map
}

// Subscribe registers the provided listener as a subscriber to future published messages.
//
// Subscribe spawns a goroutine that lives as long as the listener.
func (ps *Router) Subscribe(listener func(<-chan string)) {
	subCh := make(chan string)
	id := uuid.New()
	ps.subscribers.Store(id, (chan<- string)(subCh))

	go func() {
		listener(subCh)
		ps.subscribers.Delete(id)
		log.Printf("Removed subscriber %s", id)
	}()
	log.Printf("Added subscriber %s", id)
}

// Publish sends the provided message to all active subscribers.
func (ps *Router) Publish(ctx context.Context, msg string) error {
	var err error
	ps.subscribers.Range(func(_, value any) bool {
		sub := value.(chan<- string)
		select {
		case sub <- msg:
		case <-ctx.Done():
			err = ctx.Err()
			return false
		}
		return true
	})
	return err
}
