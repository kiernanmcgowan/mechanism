package mechanism

import (
	"encoding/json"
	"time"

	"github.com/apex/log"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

type Enqueuer struct {
	sqsURL       string
	sqs          sqsiface.SQSAPI
	queue        chan transport
	quit         chan bool
	errc         chan error
	drainCs      []chan bool
	ts           []transport
	flushTimeout time.Duration
}

func newEnqueuer(sqsURL string, sqs sqsiface.SQSAPI) *Enqueuer {
	return &Enqueuer{
		sqsURL:       sqsURL,
		sqs:          sqs,
		queue:        make(chan transport, 10),
		quit:         make(chan bool),
		errc:         make(chan error),
		flushTimeout: time.Second,
	}
}

func (p *Enqueuer) Start() <-chan error {
	ticker := time.NewTicker(p.flushTimeout)
	go func() {
		for {
			flush := false
			select {
			case t := <-p.queue:
				p.ts = append(p.ts, t)
				flush = len(p.ts) == 10
			case <-ticker.C:
				flush = true
			case <-p.quit:
				close(p.errc)
				close(p.queue)
				return
			}

			if flush {
				p.flush()
			}
		}
	}()

	return p.errc
}

func (p *Enqueuer) Stop() {
	p.quit <- true
}

func (p *Enqueuer) flush() {
	if len(p.ts) == 0 {
		// non blocking send to channels
		for _, c := range p.drainCs {
			select {
			case c <- true:
			default:
			}
		}
	} else if len(p.ts) > 0 {
		err := p.pushJobs(p.ts)
		if err != nil {
			p.errc <- err
		}
		p.ts = nil
	}
}

func (p *Enqueuer) DrainTicker() <-chan bool {
	c := make(chan bool)
	p.drainCs = append(p.drainCs, c)
	return c
}

func (p *Enqueuer) WaitDrain() {
	c := p.DrainTicker()
	<-c
}

func (p *Enqueuer) pushJobs(ts []transport) error {
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
