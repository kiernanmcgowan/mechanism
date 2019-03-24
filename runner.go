package mechanism

import (
	"fmt"
	"sync"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"

	"github.com/google/uuid"
)

// Callback is a func(id string, j Job) that can be invoked to watch whats going through the worker
type Callback func(string, Job)

// PanicCallback is a func(interface{}) thats called when a Job panics, which the worker recovers from
type PanicCallback func(string, interface{})

// package level vars so jobs and middleware can be connected w/o the need for passing a worker around
var (
	encoderMap map[string]JobEncoder
	jobQueue   *Enqueuer
	lock       *sync.RWMutex
	callbacks  map[State][]Callback
	panicedCBs []PanicCallback
)

func init() {
	if encoderMap == nil {
		encoderMap = make(map[string]JobEncoder)
	}

	if callbacks == nil {
		callbacks = make(map[State][]Callback)
	}

	if lock == nil {
		lock = &sync.RWMutex{}
	}
}

type Worker struct {
	puller *puller

	errc       chan error
	jobCounter chan string
	quit       chan bool
}

// NewWorker creates a Worker pointed at the specified queueURL with the sqs service.
// Workers will spawn up to maxJobs goroutines to process jobs from the SQS queue.
// Note that setting maxJobs = 0 will remove parallelization of mechanism, causing one job to be processed at a time
func NewWorker(queueURL string, sqs sqsiface.SQSAPI, maxJobs int) *Worker {
	initEnqueuer(queueURL, sqs)
	return &Worker{
		puller: newPuller(queueURL, sqs),

		errc:       make(chan error),
		jobCounter: make(chan string, maxJobs),
		quit:       make(chan bool),
	}
}

func NewEnqueuer(queueURL string, sqs sqsiface.SQSAPI) *Enqueuer {
	initEnqueuer(queueURL, sqs)
	return jobQueue
}

func initEnqueuer(queueURL string, sqs sqsiface.SQSAPI) {
	if jobQueue != nil {
		return
	}
	jobQueue = newEnqueuer(queueURL, sqs)
}

// Start turns on this mechanism worker.
// The worker will start listening to the provided SQS queue and process jobs as they come in
func (w *Worker) Start() <-chan error {
	mergedErrc := merge(w.puller.start(), jobQueue.Start(), w.errc)

	go func() {
		for {
			select {
			case <-w.quit:
				w.puller.stop()
				jobQueue.Stop()
				close(w.errc)
				return
			case payload := <-w.puller.queue:
				w.decodeAndRun(payload)
			}
		}
	}()

	return mergedErrc
}

func (w *Worker) decodeAndRun(payload transport) {
	lock.Lock()
	defer lock.Unlock()

	if _, ok := encoderMap[payload.Name]; !ok {
		w.errc <- fmt.Errorf("Don't know how to deserialize job with name=%s", payload.Name)
		return
	}

	j, err := encoderMap[payload.Name].UnmarshalJob(payload.Payload)
	if err != nil {
		w.errc <- errors.Wrapf(err, "job unmarshal failed name=%s", payload.Name)
		return
	}

	go w.safeInvoke(payload.ID, j)
}

// Stop will cause this worker to stop pulling jobs for processing and will close channels for pushing jobs
// Any active jobs that are still running will continue to run
func (w *Worker) Stop() {
	w.quit <- true
}

func (w *Worker) safeInvoke(id string, j Job) {
	defer func() {
		// open up the job counter
		finishedID := <-w.jobCounter
		if err := recover(); err != nil {
			paniced(finishedID, err)
		}
	}()

	// mark the job as started
	w.jobCounter <- id
	notify(Pending, id, j)
	switch j.Invoke() {
	case Success:
		notify(Succeeded, id, j)
	case Fail:
		notify(Failed, id, j)
	}
}

// OnState registers a set of callbacks when a job passes a given State
func OnState(s State, c ...Callback) {
	lock.Lock()
	defer lock.Unlock()
	callbacks[s] = append(callbacks[s], c...)
}

func notify(s State, id string, j Job) {
	lock.Lock()
	defer lock.Unlock()
	// return if no callbacks have been registered
	if _, ok := callbacks[s]; !ok {
		return
	}
	for _, f := range callbacks[s] {
		go f(id, j)
	}
}

// OnPanic is called after a job panics and this worker recovers from it
func OnPanic(c ...PanicCallback) {
	lock.Lock()
	defer lock.Unlock()
	panicedCBs = append(panicedCBs, c...)
}

func paniced(id string, err interface{}) {
	for _, f := range panicedCBs {
		f(id, err)
	}
}

// Use registers a given middleware with mechanism's callbacks
func Use(m Middleware) {
	for s, cbs := range m.Hooks() {
		OnState(s, cbs...)
	}

	OnPanic(m.WhenPanic()...)
}

// RegisterJobEncoder provides mechanism with a JobEncoder for a given namespace.
// JobEncoders MUST be registered with the same name where jobs are sent and received so that they can be routed to the proper JobEncoder.
// Returns a send only channel for pushing jobs onto the queue or an error if there is already a job with the given name registered
// Use this send only channel for enqueueing jobs.
func RegisterJobEncoder(name string, e JobEncoder) (chan<- Job, error) {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := encoderMap[name]; ok {
		return nil, fmt.Errorf("Job with name=%s already registered", name)
	}

	encoderMap[name] = e

	c := make(chan Job)
	go func() {
		for j := range c {
			lock.Lock()
			payload, err := encoderMap[name].MarshalJob(j)
			lock.Unlock()
			if err != nil {
				err = errors.Wrapf(err, "job unmarshal failed name=%s", name)
				log.WithError(err).Info("skipping job")
				continue
			}
			id := uuid.New().String()
			jp := transport{
				Name:    name,
				ID:      id,
				Payload: payload,
			}
			notify(Enqueued, id, j)
			jobQueue.queue <- jp
		}
	}()

	return c, nil
}

// MustHaveEncoder asserts that the given job names have been registered. Mechanism will panic if a given job has not been register.
// This is useful for double checking that all necessary jobs have been registered before pull jobs from the queue.
func MustHaveEncoder(names ...string) {
	for _, name := range names {
		if _, ok := encoderMap[name]; !ok {
			panic(fmt.Errorf("MustHaveEncoder - no encoder for %s has been registered", name))
		}
	}
}

func merge(cs ...<-chan error) <-chan error {
	out := make(chan error)
	var wg sync.WaitGroup
	wg.Add(len(cs))
	for _, c := range cs {
		go func(c <-chan error) {
			for v := range c {
				out <- v
			}
			wg.Done()
		}(c)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
