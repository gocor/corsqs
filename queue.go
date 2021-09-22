package corsqs

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// QueueReceiver ...
type QueueReceiver interface {
	// DecodeBody of a message
	DecodeBody(ctx context.Context, body string, v interface{}) error
	// Delete a message
	Delete(ctx context.Context, receiptHandle string) error
	// Receive Messages from queue
	Receive(ctx context.Context) ([]*sqs.Message, error)
}

// QueueSender ...
type QueueSender interface {
	// SendMessage sends the raw SendMessageInput to the configurd queue
	SendMessage(ctx context.Context, input *sqs.SendMessageInput) (string, error)
	// Send just a body across
	Send(ctx context.Context, body interface{}) (string, error)
}

type queue struct {
	client *sqs.SQS
	config QueueConfig
}

// NewReceiver ...
func NewReceiver(sess *session.Session, config QueueConfig) QueueReceiver {
	return newQueue(sess, config)
}

// NewSender ...
func NewSender(sess *session.Session, config QueueConfig) QueueSender {
	return newQueue(sess, config)
}

func newQueue(sess *session.Session, config QueueConfig) *queue {
	// set defaults on config
	if config.ContextTimeoutDuration == 0 {
		config.ContextTimeoutDuration = defaultContextTimeoutDuration
	}
	if len(config.Encoding) == 0 {
		config.Encoding = QueueEncodingJSON
	}
	if config.MaxNumberOfMessages == 0 {
		config.MaxNumberOfMessages = 1
	}

	return &queue{
		client: sqs.New(sess),
		config: config,
	}
}

// SendMessage sends the raw SendMessageInput to the configurd queue
func (q *queue) SendMessage(ctx context.Context, input *sqs.SendMessageInput) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, q.config.ContextTimeoutDuration)
	defer cancel()

	input.QueueUrl = aws.String(q.config.URL)

	res, err := q.client.SendMessageWithContext(ctx, input)
	if err != nil {
		return "", fmt.Errorf("send: %w", err)
	}

	return *res.MessageId, nil
}

// Send just a body across
func (q *queue) Send(ctx context.Context, body interface{}) (string, error) {
	encoded, err := q.encodeBody(ctx, body)
	if err != nil {
		return "", err
	}
	return q.SendMessage(ctx, q.buildMessageInput(encoded))
}

// Receive Messages from queue
func (q *queue) Receive(ctx context.Context) ([]*sqs.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*time.Duration(q.config.ReceiveMessageWaitTimeSeconds+5))
	defer cancel()

	res, err := q.client.ReceiveMessageWithContext(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(q.config.URL),
		MaxNumberOfMessages: aws.Int64(q.config.MaxNumberOfMessages),
		WaitTimeSeconds:     aws.Int64(q.config.ReceiveMessageWaitTimeSeconds),
		VisibilityTimeout:   q.config.VisibilityTimeoutSeconds,
		// MessageAttributeNames: aws.StringSlice([]string{"All"}),
	})
	if err != nil {
		return nil, fmt.Errorf("receive: %w", err)
	}

	return res.Messages, nil
}

// Delete a message
func (q *queue) Delete(ctx context.Context, receiptHandle string) error {
	ctx, cancel := context.WithTimeout(ctx, q.config.ContextTimeoutDuration)
	defer cancel()

	if _, err := q.client.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      aws.String(q.config.URL),
		ReceiptHandle: aws.String(receiptHandle),
	}); err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	return nil
}

// DecodeBody ...
func (q *queue) DecodeBody(ctx context.Context, body string, v interface{}) error {
	if q.config.Encoding == QueueEncodingJSON {
		return q.decodeJSON(ctx, body, v)
	} else {
		return fmt.Errorf("invalid encoding type=%s", q.config.Encoding)
	}
}

func (q *queue) decodeJSON(ctx context.Context, body string, v interface{}) error {
	if err := json.Unmarshal([]byte(body), v); err != nil {
		return err
	}
	return nil
}

func (q *queue) buildMessageInput(body string) *sqs.SendMessageInput {
	return &sqs.SendMessageInput{
		DelaySeconds: aws.Int64(q.config.DelaySeconds),
		MessageBody:  aws.String(body),
		QueueUrl:     aws.String(q.config.URL),
	}
}

func (q *queue) encodeBody(ctx context.Context, body interface{}) (string, error) {
	if q.config.Encoding == QueueEncodingJSON {
		return q.encodeJSON(ctx, body)
	} else if q.config.Encoding == QueueEncodingRaw {
		return fmt.Sprintf("%v", body), nil
	} else {
		return "", fmt.Errorf("invalid encoding type=%s", q.config.Encoding)
	}
}

func (q *queue) encodeJSON(ctx context.Context, body interface{}) (string, error) {
	bytes, err := json.Marshal(body)
	if err != nil {
		return "", fmt.Errorf("failed to marshal json: %w", err)
	}
	return string(bytes), nil
}
