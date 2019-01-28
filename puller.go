package mechanism

import (
	"encoding/json"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/jpillora/backoff"
)

type puller struct {
	sqsURL  string
	sqs     sqsiface.SQSAPI
	queue   chan transport
	quit    chan bool
	backoff *backoff.Backoff
}

func newPuller(sqsURL string, sqs sqsiface.SQSAPI) *puller {
	return &puller{
		sqsURL: sqsURL,
		sqs:    sqs,
		queue:  make(chan transport, 10), // sqs returns at most 10, so don't risk dropping more than needed
		quit:   make(chan bool),
		backoff: &backoff.Backoff{
			Min:    time.Second,
			Max:    10 * time.Minute,
			Factor: 2,
		},
	}
}

func (p *puller) start() <-chan error {
	cerr := make(chan error)
	go func() {
		for {
			select {
			case <-p.quit:
				close(cerr)
				close(p.queue)
				return
			default:
				jobs, err := p.pollQueue()
				if err != nil {
					cerr <- err
					continue
				}

				if len(jobs) == 0 {
					d := p.backoff.Duration()
					log.Debugf("no jobs, sleeping for duration=%s", d.String())
					time.Sleep(d)
				} else {
					p.backoff.Reset()
					for _, j := range jobs {
						p.queue <- j
					}
				}
			}
		}

	}()
	return cerr
}

func (p *puller) stop() {
	p.quit <- true
}

func (p *puller) pollQueue() ([]transport, error) {
	log.Debug("polling queue")
	resp, err := p.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(p.sqsURL),
		MaxNumberOfMessages: aws.Int64(10), // always grab the most messages possible
	})

	if err != nil {
		return nil, err
	}

	if len(resp.Messages) == 0 {
		return nil, nil
	}

	var out []transport
	var entries []*sqs.DeleteMessageBatchRequestEntry
	for _, m := range resp.Messages {
		var t transport
		err := json.Unmarshal([]byte(*m.Body), &t)
		if err != nil {
			return nil, err
		}

		out = append(out, t)

		entries = append(entries, &sqs.DeleteMessageBatchRequestEntry{
			Id:            m.MessageId,
			ReceiptHandle: m.ReceiptHandle,
		})
	}

	_, err = p.sqs.DeleteMessageBatch(&sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(p.sqsURL),
		Entries:  entries,
	})

	if err != nil {
		return nil, err
	}

	return out, nil
}
