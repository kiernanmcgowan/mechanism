package mechanism

import (
	"encoding/json"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type pusher struct {
	sqsURL       string
	sqs          sqsiface.SQSAPI
	queue        chan transport
	quit         chan bool
	flushTimeout time.Duration
}

func newPusher(sqsURL string, sqs sqsiface.SQSAPI) *pusher {
	return &pusher{
		sqsURL:       sqsURL,
		sqs:          sqs,
		queue:        make(chan transport, 10),
		quit:         make(chan bool),
		flushTimeout: time.Second,
	}
}

func (p *pusher) start() <-chan error {
	var ts []transport
	ticker := time.NewTicker(p.flushTimeout)
	errc := make(chan error)
	go func() {
		for {
			select {
			case t := <-p.queue:
				ts = append(ts, t)
				if len(ts) == 10 {
					err := p.pushJobs(ts)
					if err != nil {
						errc <- err
						continue
					}
					ts = nil
				}
			case <-ticker.C:
				if len(ts) > 0 {
					err := p.pushJobs(ts)
					if err != nil {
						errc <- err
						continue
					}
					ts = nil
				}
			case <-p.quit:
				close(errc)
				close(p.queue)
				return
			}

		}
	}()

	return errc
}

func (p *pusher) stop() {
	p.quit <- true
}

func (p *pusher) pushJobs(ts []transport) error {
	log.Debugf("pushing jobs count=%d", len(ts))
	var entities []*sqs.SendMessageBatchRequestEntry

	for _, t := range ts {
		b, err := json.Marshal(t)
		if err != nil {
			return err
		}
		entities = append(entities, &sqs.SendMessageBatchRequestEntry{
			Id:          aws.String(t.ID),
			MessageBody: aws.String(string(b[:])),
		})
	}

	_, err := p.sqs.SendMessageBatch(&sqs.SendMessageBatchInput{
		QueueUrl: aws.String(p.sqsURL),
		Entries:  entities,
	})

	return err
}
