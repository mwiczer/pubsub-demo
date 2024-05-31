package pubsub

import (
	"context"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

func TestRunPubSub(t *testing.T) {
	type testCase struct {
		numSubs     int
		numMessages int
	}
	testCases := make([]testCase, 0, 16)
	for numSubs := 0; numSubs < 5; numSubs++ {
		for messages := 0; messages < 5; messages++ {
			testCases = append(testCases, testCase{
				numSubs:     numSubs,
				numMessages: messages,
			})
		}
	}
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%d-receivers_%d-messages", tc.numSubs, tc.numMessages), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			wg := sync.WaitGroup{}

			subs := make([]chan<- string, tc.numSubs)
			received := make([][]string, tc.numSubs)
			for i := 0; i < tc.numSubs; i++ {
				i := i
				ch := make(chan string)
				subs[i] = ch
				msgs := make([]string, 0, tc.numMessages)
				wg.Add(1)
				go func() {
					defer func() {
						received[i] = msgs
						wg.Done()
					}()
					for {
						select {
						case msg := <-ch:
							msgs = append(msgs, msg)
						case <-ctx.Done():
							return
						}
					}
				}()
			}

			pubCh, doneCh := RunPubSub(ctx, subs...)
			published := make([]string, 0, tc.numMessages)
			for i := 0; i < tc.numMessages; i++ {
				msg := fmt.Sprintf("Message #%d", i)
				pubCh <- msg
				published = append(published, msg)
			}
			close(pubCh)
			<-doneCh
			cancel()
			wg.Wait()

			for i := 0; i < tc.numSubs; i++ {
				if recvd := len(received[i]); recvd != tc.numMessages {
					t.Errorf("Receiver[%d] received %d messages; want %d", i, recvd, tc.numMessages)
				}
				if diff := cmp.Diff(published, received[i]); diff != "" {
					t.Errorf("Receiver[%d] did not receive the published messages (-want +got)\n%s", i, diff)
				}
			}
		})
	}
}

func TestRunPubSub_EarlyExit(t *testing.T) {
	t.Skip("The runner implementation does not support a subscriber returning early. Un-skip this test to observe its failure.")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	numMessages := 10
	subEarlyExit := make(chan string)
	subscribeToAll := make(chan string)

	wg := sync.WaitGroup{}

	received := make([]string, 0, numMessages)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg := <-subscribeToAll:
				received = append(received, msg)
			case <-ctx.Done():
				return
			}
		}
	}()

	// This subscriber exits after receiving the first message
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case msg := <-subEarlyExit:
			log.Printf("Second subscriber exiting after receiving message %q", msg)
		case <-ctx.Done():
		}
	}()

	pubCh, doneCh := RunPubSub(ctx, subEarlyExit, subscribeToAll)
	published := make([]string, 0, numMessages)
PublishLoop:
	for i := 0; i < numMessages; i++ {
		msg := fmt.Sprintf("Message #%d", i)
		select {
		case pubCh <- msg:
		case <-ctx.Done():
			t.Errorf("Context ended before publishing message #%d: %v", i, ctx.Err())
			break PublishLoop
		}
		published = append(published, msg)
	}
	close(pubCh)
	<-doneCh
	cancel()
	wg.Wait()

	if recvd := len(received); recvd != numMessages {
		t.Errorf("Receiver received %d messages; want %d", recvd, numMessages)
	}
	if diff := cmp.Diff(published, received); diff != "" {
		t.Errorf("Receiver did not receive the published messages (-want +got)\n%s", diff)
	}
}
