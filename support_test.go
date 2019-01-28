package mechanism

import (
	"encoding/json"
	"fmt"
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
	for _, e := range input.Entries {
		delete(m.Jobs, *e.Id)
	}
	return nil, nil
}

func (m *mockedSQS) SendMessageBatch(input *sqs.SendMessageBatchInput) (*sqs.SendMessageBatchOutput, error) {
	if m.SendMessageError != nil {
		return nil, m.SendMessageError
	}
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

type TestJob struct{ Resolve Status }

func (s TestJob) Invoke() Status {
	time.Sleep(time.Second)
	return s.Resolve
}

type PanicJob struct{}

func (p PanicJob) Invoke() Status {
	panic("we're going down!")
}
