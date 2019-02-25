package mechanism

import (
	"fmt"
	"sync"
	"testing"
)

func Test_SimpleEndToEnd(t *testing.T) {
	clearMiddleware()
	w := initWorker("", &mockedSQS{}, 1)

	wg := sync.WaitGroup{}

	var jobID string
	wg.Add(1)
	OnState(Enqueued, func(id string, j Job) {
		jobID = id
		wg.Done()
	})

	wg.Add(1)
	OnState(Pending, func(id string, j Job) {
		wg.Done()
		if id != jobID {
			t.Errorf("OnState - Pending passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	wg.Add(1)
	OnState(Succeeded, func(id string, j Job) {
		wg.Done()
		if id != jobID {
			t.Errorf("OnState - Succeeded passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	job := TestJob{Resolve: Success}
	job.Enqueue()

	errc := w.Run()

	wg.Wait()

	w.Stop()

	err := <-errc
	if err != nil {
		t.Fatalf("Got error from worker %v", err)
	}
}

func clearMiddleware() {
	fmt.Println("cleared")
	callbacks = make(map[State][]Callback)
	panicedCBs = nil
}
