package corsqs

import (
	"context"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/google/uuid"
	"github.com/jpillora/backoff"
	"gitlab.com/gocor/corctx"
	"gitlab.com/gocor/corlog"
)

const defBackoffMinMS = 250
const defBackoffMaxMS = 10000 // 10 sec

// WorkerConfig ...
type WorkerConfig struct {
	// Name ...
	Name string
	// IsAsync determines if messages should be processed async in separte goroutines, or synchronously
	IsAsync bool
	// LogMessage ...
	LogMessage bool
	// MaxWorkers that will independently receive messages from a queue.
	MaxWorkers int
	// BackoffMinMS
	BackoffMinMS int
	// BackoffMaxMS
	BackoffMaxMS int
}

type Worker interface {
	// Start Worker ...
	Start(ctx context.Context, handler func(ctx context.Context, msg *sqs.Message) error)
	// DecodeBody of a message
	DecodeBody(ctx context.Context, body string, v interface{}) error
}

type worker struct {
	receiver    QueueReceiver
	config      WorkerConfig
	queueConfig QueueConfig
	handler     func(ctx context.Context, msg *sqs.Message) error
}

// NewWorker ...
func NewWorker(sess *session.Session, config WorkerConfig, queueConfig QueueConfig) Worker {
	// set config defaults
	if config.BackoffMinMS == 0 {
		config.BackoffMinMS = defBackoffMinMS
	}
	if config.BackoffMaxMS == 0 {
		config.BackoffMaxMS = defBackoffMaxMS
	}

	receiver := NewReceiver(sess, queueConfig)
	return &worker{
		receiver:    receiver,
		config:      config,
		queueConfig: queueConfig,
	}
}

// Start Worker ...
func (w *worker) Start(ctx context.Context, handler func(ctx context.Context, msg *sqs.Message) error) {
	w.handler = handler

	wg := &sync.WaitGroup{}
	wg.Add(w.config.MaxWorkers)

	for i := 1; i <= w.config.MaxWorkers; i++ {
		go w.run(ctx, wg, i)
	}

	wg.Wait()
}

func (w *worker) run(ctx context.Context, wg *sync.WaitGroup, id int) {
	defer wg.Done()
	l := corlog.New(ctx)
	l.Infow("run started", "id", id, "name", w.config.Name)

	backoffCfg := w.getBackoffConfig(ctx)

	for {
		select {
		case <-ctx.Done():
			l.Infow("run stopped", "id", id)
			return
		default:
		}

		msgs, err := w.receiver.Receive(ctx)
		// handle backoff if not long polling and err or no messages
		if w.queueConfig.ReceiveMessageWaitTimeSeconds == 0 &&
			(err != nil || len(msgs) == 0) {
			time.Sleep(backoffCfg.Duration())
		}
		if err != nil {
			// Critical error!
			l.Errorw("Worker Error", "err", err.Error(), "id", id, "name", w.config.Name)
			continue
		}
		if len(msgs) == 0 {
			continue
		}

		// reset backoff
		backoffCfg.Reset()
		if w.config.IsAsync {
			w.async(ctx, msgs)
		} else {
			w.sync(ctx, msgs)
		}
	}
}

func (w *worker) sync(ctx context.Context, msgs []*sqs.Message) {
	for _, msg := range msgs {
		w.processMessage(ctx, msg)
	}
}

func (w *worker) async(ctx context.Context, msgs []*sqs.Message) {
	wg := &sync.WaitGroup{}
	wg.Add(len(msgs))

	for _, msg := range msgs {
		go func(msg *sqs.Message) {
			defer wg.Done()

			w.processMessage(ctx, msg)
		}(msg)
	}

	wg.Wait()
}

func (w *worker) getBackoffConfig(ctx context.Context) backoff.Backoff {
	return backoff.Backoff{
		Factor: 1,
		Min:    time.Duration(w.config.BackoffMinMS) * time.Millisecond,
		Max:    time.Duration(w.config.BackoffMaxMS) * time.Millisecond,
		Jitter: true,
	}
}

func (w *worker) processMessage(ctx context.Context, msg *sqs.Message) {
	l := corlog.New(ctx)
	// recover but don't delete for retry
	defer func() {
		if r := recover(); r != nil {
			l.Infow("Recovered process message", "receiptHandle", msg.ReceiptHandle)
		}
	}()

	reqID := uuid.New().String()
	newCtx := corctx.WithRequestID(ctx, reqID)

	// process handler but continue
	if err := w.handler(newCtx, msg); err != nil {
		return
	}

	if err := w.receiver.Delete(newCtx, *msg.ReceiptHandle); err != nil {
		l.Errorw("delete error", "err", err.Error(), "receiptHandle", *msg.ReceiptHandle)
	}
}

// DecodeBody of a message
func (w *worker) DecodeBody(ctx context.Context, body string, v interface{}) error {
	return w.receiver.DecodeBody(ctx, body, v)
}
