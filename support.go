package mechanism

// Status decribes how a job performed and what should be done with the job itself
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

// Serializer prepares a job for being queued
type Serializer = func(Job) ([]byte, error)

// Deserializer takes a JSON struct from a SQS payload and contructs a valid job from it
type Deserializer = func([]byte) (Job, error)

type transport struct {
	Name    string `json:"name"`
	ID      string `json:"id"`
	Payload []byte `json:"payload"`
}
