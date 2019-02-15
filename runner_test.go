package mechanism

import (
	"testing"
	"time"
)

func Test_WorkerRun(t *testing.T) {
	w := NewWorker("", &mockedSQS{}, 1)
	cerr := w.Run()
	w.Stop()
	err := <-cerr
	if err != nil {
		t.Fatalf("error is not nil %v", err)
	}
}

func Test_SafeEnvokePassingJob(t *testing.T) {
	w := NewWorker("", &mockedSQS{}, 1)

	jobID := "job id"
	job := TestJob{Resolve: Success}

	successCalled, invokeCalled := false, false
	w.OnInvoke(func(id string, j Job) {
		invokeCalled = true
		if id != jobID {
			t.Errorf("OnInvoke passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	w.OnSuccess(func(id string, j Job) {
		successCalled = true
		if id != jobID {
			t.Errorf("OnSuccess passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	w.safeInvoke(jobID, job)

	defer func() {
		if !invokeCalled {
			t.Fatal("OnInvoke was never invoked")
		}
		if !successCalled {
			t.Fatal("OnSuccess was never invoked")
		}
	}()
}

func Test_SafeEnvokeFailingJob(t *testing.T) {
	w := NewWorker("", &mockedSQS{}, 1)

	jobID := "job id"
	job := TestJob{Resolve: Fail}

	failedCalled, invokeCalled := false, false
	w.OnInvoke(func(id string, j Job) {
		invokeCalled = true
		if id != jobID {
			t.Errorf("OnInvoke passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	w.OnFail(func(id string, j Job) {
		failedCalled = true
		if id != jobID {
			t.Errorf("OnFail passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	w.safeInvoke(jobID, job)

	defer func() {
		if !invokeCalled {
			t.Fatal("OnInvoke was never invoked")
		}
		if !failedCalled {
			t.Fatal("OnFail was never invoked")
		}
	}()
}

func Test_SafeEnvokePanicJob(t *testing.T) {
	w := NewWorker("", &mockedSQS{}, 1)

	jobID := "job id"
	job := PanicJob{}

	wasInvoked := false
	w.OnPanic(func(err interface{}) {
		wasInvoked = true
	})

	w.safeInvoke(jobID, job)

	defer func() {
		if !wasInvoked {
			t.Fatal("OnFail was never invoked")
		}
	}()
}

func Test_RegisterJob(t *testing.T) {
	w := NewWorker("", &mockedSQS{}, 1)

	_, err := registerSampleTransport(w)

	if err != nil {
		t.Fatal("RegisterJob returned nil new a new register")
	}

	_, err = registerSampleTransport(w)

	if err == nil {
		t.Fatal("RegisterJob did not return nil for a duplicate register")
	}
}

func Test_EnqueueJob(t *testing.T) {
	w := NewWorker("", &mockedSQS{}, 1)

	c, err := registerSampleTransport(w)
	if err != nil {
		t.Fatalf("Failed to register transport %v", err)
	}

	var enqueueCalled bool
	w.OnEnqueue(func(id string, j Job) {
		enqueueCalled = true
		if id == "" {
			t.Fatal("ID is empty string")
		}
	})

	job := TestJob{Resolve: Success}
	c <- job

	if err != nil {
		t.Fatalf("Failed to enqueue %v", err)
	}

	defer func() {
		if !enqueueCalled {
			t.Fatal("OnEnqueue was never invoked")
		}

		if len(w.pusher.queue) != 1 {
			t.Fatal("pusher did not receive object")
		}
	}()
}

func Test_SimpleEndToEnd(t *testing.T) {
	w := NewWorker("", &mockedSQS{}, 1)

	var jobID string
	enqueueCalled, successCalled, invokeCalled := false, false, false
	w.OnEnqueue(func(id string, j Job) {
		enqueueCalled = true
		jobID = id
	})
	w.OnInvoke(func(id string, j Job) {
		invokeCalled = true
		if id != jobID {
			t.Errorf("OnInvoke passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	w.OnSuccess(func(id string, j Job) {
		successCalled = true
		if id != jobID {
			t.Errorf("OnSuccess passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	c, err := registerSampleTransport(w)
	if err != nil {
		t.Fatalf("Failed to register transport %v", err)
	}
	job := TestJob{Resolve: Success}
	c <- job

	cerr := w.Run()
	go func() {
		time.Sleep(5 * time.Second)
		w.Stop()
	}()
	err = <-cerr
	if err != nil {
		t.Fatalf("Got error from worker %v", err)
	}
	defer func() {
		if !enqueueCalled {
			t.Fatal("OnEnqueue was never invoked")
		}
		if !invokeCalled {
			t.Fatal("OnInvoke was never invoked")
		}
		if !successCalled {
			t.Fatal("OnSuccess was never invoked")

		}
	}()
}

func registerSampleTransport(w *Worker) (chan<- Job, error) {
	p := &porter{}
	return w.RegisterTransporter("sample", p)
}
