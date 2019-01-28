package mechanism

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func Test_PusherBasic(t *testing.T) {
	p := newPusher("", &mockedSQS{})
	cerr := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	err := <-cerr
	if err != nil {
		t.Errorf("Recieved not nil error from listen %v", err)
	}
}

func Test_PusherMessages(t *testing.T) {
	p := newPusher("", &mockedSQS{})
	cerr := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	for i := 0; i < 10; i++ {
		p.queue <- transport{ID: strconv.Itoa(i)}
	}

	err := <-cerr
	if err != nil {
		t.Errorf("Recieved not nil error from listen %v", err)
	}

}

func Test_PusherSendError(t *testing.T) {
	p := newPusher("", &mockedSQS{SendMessageError: fmt.Errorf("beep boop")})
	cerr := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	for i := 0; i < 10; i++ {
		p.queue <- transport{ID: strconv.Itoa(i)}
	}

	err := <-cerr
	if err == nil {
		t.Fatal("Recieved nil as an error when one was expected")
	}
	if err.Error() != "beep boop" {
		t.Fatalf("Recieved wrong error from channel %v", err)
	}

}

func Test_PusherSendErrorWhileIdle(t *testing.T) {
	p := newPusher("", &mockedSQS{SendMessageError: fmt.Errorf("beep boop")})
	cerr := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	p.queue <- transport{ID: "1"}

	err := <-cerr
	if err == nil {
		t.Fatal("Recieved nil as an error when one was expected")
	}
	if err.Error() != "beep boop" {
		t.Fatalf("Recieved wrong error from channel %v", err)
	}

}
