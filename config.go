package corsqs

import "time"

// QueueEncodingType ...
type QueueEncodingType string

const (
	// QueueEncodingRaw ...
	QueueEncodingRaw QueueEncodingType = "raw"
	// QueueEncodingJSON ...
	QueueEncodingJSON QueueEncodingType = "json"
)

// QueueConfig ...
type QueueConfig struct {
	// Encoding ...
	Encoding QueueEncodingType
	// URL for the Queue
	URL string
	// DelaySeconds (0-900, 15 minutes)
	DelaySeconds int64
	// ReceiveMessageWaitTimeSeconds (0-20)
	ReceiveMessageWaitTimeSeconds int64
	// ContextTimeoutDuration is time for context cancellation on right
	ContextTimeoutDuration time.Duration
	// VisibilityTimeoutseconds (0 to 43,200, 12 hours)
	VisibilityTimeoutSeconds *int64
	// MaxNumberOfMessages on read
	MaxNumberOfMessages int64
}

const defaultContextTimeoutDuration = time.Second * 30
