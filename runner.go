package mechanism

import (
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"

	"github.com/google/uuid"
)

type serde struct {
	Ser Serializer
	De  Deserializer
}

// Callback is a func(id string, j Job) that can be invoked to watch whats going through the worker
type Callback func(string, Job)

// PanicCallback is a func(interface{}) thats called when a Job panics, which the worker recovers from
type PanicCallback func(interface{})

// Worker manages pulling or enqueuing jobs from SQS
type Worker struct {
	serdeMap map[string]serde
	puller   *puller
	pusher   *pusher

	errChan    chan error
	jobCounter chan bool
	quit       chan bool

	invokeCBs  []Callback
	successCBs []Callback
	failCBs    []Callback
	panicedCBs []PanicCallback

	lock *sync.RWMutex
}

// NewWorker creates a Worker pointed at the specified queueURL with the sqs service.
// Workers will spawn up to maxJobs goroutines to process jobs from the SQS queue.
// Note that setting maxJobs = 0 will remove parallelization of mechanism, causing one job to be processed at a time
func NewWorker(queueURL string, sqs sqsiface.SQSAPI, maxJobs int) *Worker {
	return &Worker{
		serdeMap: make(map[string]serde),
		puller:   newPuller(queueURL, sqs),
		pusher:   newPusher(queueURL, sqs),

		errChan:    make(chan error),
		jobCounter: make(chan bool, maxJobs),
		quit:       make(chan bool),

		lock: &sync.RWMutex{},
	}
}

// Run turns on this mechanism worker.
// The worker will start listening to the provided SQS queue and process jobs as they come in
func (w *Worker) Run() <-chan error {
	w.start()

	go func() {
		for {
			select {
			case <-w.quit:
				w.puller.stop()
				w.pusher.stop()
				close(w.errChan)
				return
			case payload := <-w.puller.queue:
				if _, ok := w.serdeMap[payload.Name]; !ok {
					w.errChan <- fmt.Errorf("Don't know how to deserialize job with name=%s", payload.Name)
					continue
				}

				j, err := w.serdeMap[payload.Name].De(payload.Payload)
				if err != nil {
					w.errChan <- err
					continue
				}

				go w.safeInvoke(payload.ID, j)
			}
		}
	}()

	return w.errChan
}

// Stop will cause this worker to stop pulling jobs for processing and will close channels for pushing jobs
// Any active jobs that are still running will continue to run
func (w *Worker) Stop() {
	w.quit <- true
}

func (w *Worker) safeInvoke(id string, j Job) {
	defer func() {
		// open up the job counter
		<-w.jobCounter
		if err := recover(); err != nil {
			w.paniced(err)
		}
	}()

	// mark the job as started
	w.jobCounter <- true
	w.invoked(id, j)
	switch j.Invoke() {
	case Success:
		w.successful(id, j)
	case Fail:
		w.failed(id, j)
	}
}

func (w *Worker) start() {
	go func() {
		for e := range w.puller.start() {
			w.errChan <- e
		}
	}()

	go func() {
		for e := range w.pusher.start() {
			w.errChan <- e
		}
	}()
}

// OnInvoke is called just before a job's Invoke func is called
func (w *Worker) OnInvoke(c ...Callback) {
	w.lock.Lock()
	w.invokeCBs = append(w.invokeCBs, c...)
	w.lock.Unlock()
}

func (w *Worker) invoked(id string, j Job) {
	for _, f := range w.invokeCBs {
		f(id, j)
	}
}

// OnSuccess is called just after a job's Invoke func returns with a Success
func (w *Worker) OnSuccess(c ...Callback) {
	w.lock.Lock()
	w.successCBs = append(w.successCBs, c...)
	w.lock.Unlock()
}

func (w *Worker) successful(id string, j Job) {
	for _, f := range w.successCBs {
		f(id, j)
	}
}

// OnFail is called just after a job's Invoke func returns with a Fail
func (w *Worker) OnFail(c ...Callback) {
	w.lock.Lock()
	w.failCBs = append(w.failCBs, c...)
	w.lock.Unlock()
}

func (w *Worker) failed(id string, j Job) {
	for _, f := range w.failCBs {
		f(id, j)
	}
}

// OnPanic is called after a job panics and this worker recovers from it
func (w *Worker) OnPanic(c ...PanicCallback) {
	w.lock.Lock()
	w.panicedCBs = append(w.panicedCBs, c...)
	w.lock.Unlock()
}

func (w *Worker) paniced(err interface{}) {
	for _, f := range w.panicedCBs {
		f(err)
	}
}

// Enqueue will serialize a Job j with Serializer attached to name and enqueue it
// The transport id is returned if successfully pushed seralized, otherwise ("", error) is returned
func (w *Worker) Enqueue(name string, j Job) (string, error) {
	if _, ok := w.serdeMap[name]; !ok {
		return "", fmt.Errorf("Don't know how to serialize job with name=%s", name)
	}

	payload, err := w.serdeMap[name].Ser(j)
	if err != nil {
		return "", err
	}
	id := uuid.New().String()
	jp := transport{
		Name:    name,
		ID:      id,
		Payload: payload,
	}
	w.pusher.queue <- jp
	return id, nil
}

// RegisterJob provides mechanism with the seralization/deserialization needed to transport jobs across an external queue
// Returns an error if there is already a job with the given name registered
func (w *Worker) RegisterJob(name string, ser Serializer, de Deserializer) error {
	// guard against accidental panics
	if w.serdeMap == nil {
		w.serdeMap = make(map[string]serde)
	}
	if _, ok := w.serdeMap[name]; ok {
		return fmt.Errorf("Job with name=%s already registerd", name)
	}

	w.lock.Lock()
	w.serdeMap[name] = serde{
		Ser: ser,
		De:  de,
	}
	w.lock.Unlock()

	return nil
}
