package mechanism

// Status describes how a job performed and what should be done with the job itself
type Status int

const (
	// Success will invoke OnSuccess callbacks after a job exits
	Success Status = iota
	// Fail will invoke OnFail callbacks after a job exits
	Fail
)

// Job is how work is performed and tracked inside of mechaism
type Job interface {
	// Invoke will be run inside of a recoverable goroutine and should return Success or Fail
	// based on the result of the job. Based on the return status OnSuccess or OnFail callbacks will run
	Invoke() Status
}

// Transporter encodes and decodes jobs for safe transport across the wire
// It consists of two methods that must be inverses of each other to ensure smooth operation of the job queue.
// In practice these will often be json.Marshal and json.Unmarshal pairs, but the implementation is left open to the user.
type Transporter interface {
	// Marshal is called by mechanism when a job is ready to be sent to the queue.
	// The returned []byte will be sent with additional information to help with Unmarshaling on the other end of the wire.
	// It must return a []byte that can be Unmarshaled or return an error. Errors are sent to the worker's error channel returned by Run().
	Marshal(Job) ([]byte, error)

	// Unmarshal is called by mechanism when a job is pulled off the queue that was serialized by this Transporter's Serialize func.
	// It must create a job that can be run or produce an error. Error are sent to the worker's error channel returned by Run().
	Unmarshal([]byte) (Job, error)
}

type transport struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}
