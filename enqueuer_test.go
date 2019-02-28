package mechanism

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func Test_PusherBasic(t *testing.T) {
	p := newEnqueuer("", &mockedSQS{})
	errc := p.Start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.Stop()
	}()

	err := <-errc
	if err != nil {
		t.Errorf("Received not nil error from listen %v", err)
	}
}

func Test_PusherMessages(t *testing.T) {
	p := newEnqueuer("", &mockedSQS{})
	errc := p.Start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.Stop()
	}()

	for i := 0; i < 10; i++ {
		p.queue <- transport{ID: strconv.Itoa(i)}
	}

	err := <-errc
	if err != nil {
		t.Errorf("Received not nil error from listen %v", err)
	}

}

func Test_PusherSendError(t *testing.T) {
	p := newEnqueuer("", &mockedSQS{SendMessageError: fmt.Errorf("beep boop")})
	errc := p.Start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.Stop()
	}()

	for i := 0; i < 10; i++ {
		p.queue <- transport{ID: strconv.Itoa(i)}
	}

	err := <-errc
	if err == nil {
		t.Fatal("Received nil as an error when one was expected")
	}
	if err.Error() != "beep boop" {
		t.Fatalf("Received wrong error from channel %v", err)
	}

}

func Test_PusherSendErrorWhileIdle(t *testing.T) {
	p := newEnqueuer("", &mockedSQS{SendMessageError: fmt.Errorf("beep boop")})
	errc := p.Start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.Stop()
	}()

	p.queue <- transport{ID: "1"}

	err := <-errc
	if err == nil {
		t.Fatal("Received nil as an error when one was expected")
	}
	if err.Error() != "beep boop" {
		t.Fatalf("Received wrong error from channel %v", err)
	}

}
