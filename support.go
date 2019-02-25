package mechanism

// Status describes how a job performed and what should be done with the job itself
type Status int

const (
	// Success will invoke OnSuccess callbacks after a job exits
	Success Status = iota
	// Fail will invoke OnFail callbacks after a job exits
	Fail
)

// State is an iota representing where a job is in its lifecycle.
type State int

// A job's life cycle is Enqueued -> (pushed onto SQS) -> (pulled from SQS) -> Pending -> (job is Invoked) -> Succeeded/Failed/Paniced
// Note that Enqueued and Pending/Succeeded/Failed/Paniced may not occur on the same worker for a given job.
const (
	Enqueued State = iota
	Pending
	Succeeded
	Failed
	Paniced
)

// Job is how work is performed and tracked inside of mechanism
type Job interface {
	// Invoke will be run inside of a recoverable goroutine and should return Success or Fail
	// based on the result of the job. Based on the return status OnSuccess or OnFail callbacks will run
	Invoke() Status

	// Enqueue sends this job off to the job queue
	Enqueue()
}

// JobEncoder encodes and decodes jobs for safe transport across the wire
// It consists of two methods that must be inverses of each other to ensure smooth operation of the job queue.
// In practice these will often be json.Marshal and json.Unmarshal pairs, but the implementation is left open to the user.
type JobEncoder interface {
	// Marshal is called by mechanism when a job is ready to be sent to the queue and returns (data, error).
	// The returned []byte will be sent with additional information to help with Unmarshaling of jobs when pulled off of the job queue.
	// It must return a []byte that can be turned into a Job via Unmarshal.
	// Jobs that error when marshaled are not queued. It is up to the user to deal with errors in the Marshaling process.
	MarshalJob(Job) ([]byte, error)

	// Unmarshal is called by mechanism when a job is pulled off the queue that was serialized by this JobEncoder's Serialize func.
	// It must create a job that can be run or produce an error. Error are sent to the worker's error channel returned by Run().
	UnmarshalJob([]byte) (Job, error)
}

// Middleware is used to hook into the various callbacks that mechanism provides
type Middleware interface {
	// Hooks all the Callbacks that should be registered with mechanism
	Hooks() map[State][]Callback

	// WhenPanic returns PanicCallbacks that will be registered with mechanisms OnPanic hook
	WhenPanic() []PanicCallback
}

type transport struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}
