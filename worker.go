package corsqs

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"gitlab.com/gocor/corlog"
)

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
}

type Worker interface {
	// Start Worker ...
	Start(ctx context.Context, handler func(ctx context.Context, msg *sqs.Message) error)
	// DecodeBody of a message
	DecodeBody(ctx context.Context, body string, v interface{}) error
}

type worker struct {
	receiver QueueReceiver
	config   WorkerConfig
	handler  func(ctx context.Context, msg *sqs.Message) error
}

// NewWorker ...
func NewWorker(sess *session.Session, config WorkerConfig, queueConfig QueueConfig) Worker {
	receiver := NewReceiver(sess, queueConfig)
	return &worker{
		receiver: receiver,
		config:   config,
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

	for {
		select {
		case <-ctx.Done():
			l.Infow("run stopped", "id", id)
			return
		default:
		}

		msgs, err := w.receiver.Receive(ctx)
		if err != nil {
			// Critical error!
			l.Errorw("Worker Error", "err", err.Error(), "id", id, "name", w.config.Name)
			continue
		}

		if len(msgs) == 0 {
			continue
		}

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

func (w *worker) processMessage(ctx context.Context, msg *sqs.Message) {
	l := corlog.New(ctx)
	// recover but don't delete for retry
	defer func() {
		if r := recover(); r != nil {
			l.Infow("Recovered process message", "receiptHandle", msg.ReceiptHandle)
		}
	}()

	// process handler but continue
	if err := w.handler(ctx, msg); err != nil {
		return
	}

	if err := w.receiver.Delete(ctx, *msg.ReceiptHandle); err != nil {
		l.Errorw("delete error", "err", err.Error(), "receiptHandle", *msg.ReceiptHandle)
	}
}

// DecodeBody of a message
func (w *worker) DecodeBody(ctx context.Context, body string, v interface{}) error {
	return w.receiver.DecodeBody(ctx, body, v)
}
