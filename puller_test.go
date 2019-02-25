package mechanism

import (
	"fmt"
	"strconv"
	"testing"
	"time"
)

func Test_PullerSetupTeardown(t *testing.T) {
	// Test listening, then stopping
	p := newPuller("", &mockedSQS{})
	errc := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	err := <-errc
	if err != nil {
		t.Errorf("Received not nil error from listen %v", err)
	}
}

func Test_PullerBasic(t *testing.T) {
	p := newPuller("", &mockedSQS{Jobs: genTransport(1)})
	errc := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	var out []transport
	for j := range p.queue {
		out = append(out, j)
	}

	if len(out) != 1 {
		t.Errorf("Unexpected number of jobs returned, wanted=%d got=%d", 1, len(out))
	}

	err := <-errc
	if err != nil {
		t.Errorf("Received not nil error from listen %v", err)
	}
}

func Test_PullerBacksoff(t *testing.T) {
	p := newPuller("", &mockedSQS{Jobs: nil})
	errc := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	err := <-errc
	if err != nil {
		t.Errorf("Received not nil error from listen %v", err)
	}

	if p.backoff.Attempt() == 0 {
		t.Fatal("No backoff attempts happend")
	}

	if p.backoff.ForAttempt(p.backoff.Attempt()) < p.backoff.Min {
		t.Fatal("Backoff is has not increased with attempts")
	}
}

func Test_PullerReceiveError(t *testing.T) {
	p := newPuller("", &mockedSQS{ReceiveMessageError: fmt.Errorf("beep boop")})
	errc := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		p.stop()
	}()

	err := <-errc
	if err == nil {
		t.Fatal("Received nil as an error when one was expected")
	}
	if err.Error() != "beep boop" {
		t.Fatalf("Received wrong error from channel %v", err)
	}
}

func Test_PullerDeleteError(t *testing.T) {
	p := newPuller("", &mockedSQS{Jobs: genTransport(1), DeleteMessageError: fmt.Errorf("beep boop")})
	errc := p.start()

	// stop at some point in the future
	go func() {
		time.Sleep(time.Second)
		fmt.Println("stopping")
		p.stop()
	}()

	err := <-errc
	if err == nil {
		t.Fatal("Received nil as an error when one was expected")
	}
	if err.Error() != "beep boop" {
		t.Fatalf("Received wrong error from channel %v", err)
	}
}

func genTransport(count int) map[string]transport {
	out := make(map[string]transport)
	for i := 0; i < count; i++ {
		out[strconv.Itoa(i)] = transport{ID: strconv.Itoa(i)}
	}
	return out
}
