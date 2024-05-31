package pubsub

import (
	"context"
	"fmt"
	"sync"
	"testing"

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
