package mechanism

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type mockedSQS struct {
	sqsiface.SQSAPI
	Jobs                map[string]transport
	ReceiveMessageError error
	DeleteMessageError  error
	SendMessageError    error
}

func (m *mockedSQS) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	if m.ReceiveMessageError != nil {
		return nil, m.ReceiveMessageError
	}
	lock.Lock()
	defer lock.Unlock()
	var ms []*sqs.Message
	for _, j := range m.Jobs {
		b, _ := json.Marshal(j)
		ms = append(ms, &sqs.Message{MessageId: aws.String(j.ID), Body: aws.String(string(b))})
		if int64(len(ms)) >= *input.MaxNumberOfMessages {
			break
		}
	}
	return &sqs.ReceiveMessageOutput{Messages: ms}, nil
}

func (m *mockedSQS) DeleteMessageBatch(input *sqs.DeleteMessageBatchInput) (*sqs.DeleteMessageBatchOutput, error) {
	if m.DeleteMessageError != nil {
		return nil, m.DeleteMessageError
	}
	lock.Lock()
	defer lock.Unlock()
	for _, e := range input.Entries {
		delete(m.Jobs, *e.Id)
	}
	return nil, nil
}

func (m *mockedSQS) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	if m.SendMessageError != nil {
		return nil, m.SendMessageError
	}
	lock.Lock()
	defer lock.Unlock()
	if m.Jobs == nil {
		m.Jobs = make(map[string]transport)
	}
	for _, e := range input.Entries {
		var t transport
		json.Unmarshal([]byte(*e.MessageBody), &t)
		m.Jobs[*e.Id] = t
	}
	fmt.Printf("jobs sent to queue len=%d\n", len(m.Jobs))
	return nil, nil
}

var testc, panc chan<- Job

func init() {
	p := porter{}
	testc, _ = RegisterJobEncoder("test", p)
	panc, _ = RegisterJobEncoder("panic", p)
}

type TestJob struct{ Resolve Status }

func (s TestJob) Invoke() Status {
	time.Sleep(time.Second)
	return s.Resolve
}

func (s TestJob) Enqueue() {
	testc <- s
}

type PanicJob struct{}

func (p PanicJob) Invoke() Status {
	panic("we're going down!")
}

func (p PanicJob) Enqueue() {
	panc <- p
}

type porter struct{}

func (p porter) MarshalJob(j Job) ([]byte, error) {
	return json.Marshal(j)
}

func (p porter) UnmarshalJob(b []byte) (Job, error) {
	t := TestJob{}
	err := json.Unmarshal(b, &t)
	return t, err
}

func Test_SafeInvokePassingJob(t *testing.T) {
	clearMiddleware()
	w := NewWorker("", &mockedSQS{}, 1)

	jobID := "job id"
	job := TestJob{Resolve: Success}
	wg := sync.WaitGroup{}

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

	w.safeInvoke(jobID, job)
	wg.Wait()
}

func Test_SafeInvokeFailingJob(t *testing.T) {
	clearMiddleware()
	w := NewWorker("", &mockedSQS{}, 1)

	jobID := "job id"
	job := TestJob{Resolve: Fail}
	wg := sync.WaitGroup{}

	wg.Add(1)
	OnState(Pending, func(id string, j Job) {
		wg.Done()
		if id != jobID {
			t.Errorf("OnState - Pending passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	wg.Add(1)
	OnState(Failed, func(id string, j Job) {
		wg.Done()
		if id != jobID {
			t.Errorf("OnState - Failed passed wrong job id, got=%s wanted=%s", id, jobID)
		}
	})

	w.safeInvoke(jobID, job)
	wg.Wait()
}

func Test_SafeInvokePanicJob(t *testing.T) {
	clearMiddleware()
	w := NewWorker("", &mockedSQS{}, 1)

	jobID := "job id"
	job := PanicJob{}
	wg := sync.WaitGroup{}

	wg.Add(1)
	OnPanic(func(id string, err interface{}) {
		wg.Done()
	})

	w.safeInvoke(jobID, job)
	wg.Wait()
}

func Test_RegisterJob(t *testing.T) {
	clearMiddleware()
	p := porter{}

	_, err := RegisterJobEncoder("foo", p)

	if err != nil {
		t.Fatal("RegisterJob returned nil new a new register")
	}

	_, err = RegisterJobEncoder("foo", p)

	if err == nil {
		t.Fatal("RegisterJob did not return nil for a duplicate register")
	}
}
