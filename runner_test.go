package mechanism

import (
	"fmt"
	"sync"
	"testing"
)

func Test_SimpleEndToEnd(t *testing.T) {
	clearMiddleware()
	w := NewWorker("", &mockedSQS{}, 1)

	idLock := &sync.RWMutex{}
	wg := sync.WaitGroup{}

	var jobID string
	wg.Add(1)
	OnState(Enqueued, func(id string, j Job) {
		idLock.Lock()
		defer idLock.Unlock()
		jobID = id
		wg.Done()
	})

	wg.Add(1)
	OnState(Pending, func(id string, j Job) {
		idLock.Lock()
		defer idLock.Unlock()
		wg.Done()
		if id != jobID {
			t.Errorf("OnState - Pending passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	wg.Add(1)
	OnState(Succeeded, func(id string, j Job) {
		idLock.Lock()
		defer idLock.Unlock()
		wg.Done()
		if id != jobID {
			t.Errorf("OnState - Succeeded passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	job := TestJob{Resolve: Success}
	job.Enqueue()

	errc := w.Start()

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
