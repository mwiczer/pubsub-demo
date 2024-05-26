// pubsub-demo is a demo attempting to implement a publisher-sucriber model using go concurrency.
package main

import (
	"log"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	ch := make(chan string)

	// Listen for messages
	go func() {
		for {
			msg := <-ch
			log.Printf("Received message: %q", msg)
			time.Sleep(500 * time.Millisecond)
		}
	}()

	// Publish the messages
	messages := []string{
		"foo", "bar", "baz", "abc", "def",
	}
	for i, msg := range messages {
		log.Printf("Sending message %d...", i)
		ch <- msg
	}
	// Extra sleep to let the listener receive the last message.
	time.Sleep(500 * time.Millisecond)
}
