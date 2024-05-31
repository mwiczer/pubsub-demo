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
	mu          sync.RWMutex
	subscribers map[uuid.UUID]*subscriber
}

type subscriber struct {
	data chan<- string
	done <-chan struct{}
}

// Subscribe registers the provided listener as a subscriber to future published messages.
//
// Subscribe spawns a goroutine that lives as long as the listener.
func (ps *Router) Subscribe(listener func(<-chan string)) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if ps.subscribers == nil {
		ps.subscribers = make(map[uuid.UUID]*subscriber)
	}
	subCh := make(chan string)
	// Done channel is buffered because we know there will be at most one message in the channel.
	// Writing to the channel can be fast. We shouldn't block, as there may never be a receiver of the message.
	doneCh := make(chan struct{}, 1)
	id := uuid.New()
	ps.subscribers[id] = &subscriber{
		data: subCh,
		done: doneCh,
	}

	go func() {
		listener(subCh)
		ps.removeSubscriber(id, doneCh)
	}()
	log.Printf("Added subscriber %s", id)
}

func (ps *Router) removeSubscriber(id uuid.UUID, doneCh chan<- struct{}) {
	// Writing to the doneCh doesn't need to be in any lock, due to the inherent thread-safety of channels.
	// I don't _think_ it would be too harmful to put this inside the lock,
	// but it doesn't hurt to get the removal info out into the fanout loop more eagerly.
	doneCh <- struct{}{}

	// This doesn't cause a deadlock with the lock acquired in the main body of Subscribe
	// because we're inside a goroutine. Subscribe can return freely and release its lock before we get here.
	ps.mu.Lock()
	defer ps.mu.Unlock()
	delete(ps.subscribers, id)
	log.Printf("Removed subscriber %s", id)
}

// Publish sends the provided message to all active subscribers.
func (ps *Router) Publish(ctx context.Context, msg string) error {
	// Make a read-only copy of the subscribers list. This means that during fanout, we will ignore any new subscribers,
	// but subscribers _can_ be removed during fanout, due to our use of the done channel.
	subs := ps.cloneSubscribers()

	log.Printf("Publishing to %d active subscribers...", len(subs))
	for id, sub := range subs {
		select {
		case sub.data <- msg:
		case <-sub.done:
			// The subscribers list has shrunk since we made our read-only copy.
			log.Printf("Subscriber %v is done; skipping publish", id)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (ps *Router) cloneSubscribers() map[uuid.UUID]*subscriber {
	ps.mu.RLock()
	subs := make(map[uuid.UUID]*subscriber, len(ps.subscribers))
	for id, sub := range ps.subscribers {
		subs[id] = sub
	}
	ps.mu.RUnlock()
	return subs
}
