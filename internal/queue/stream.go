package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	StreamName     = "webhook:stream:events"
	ConsumerGroup  = "webhook:workers:group"
	EventIDKey     = "event_id"
	EndpointIDKey  = "endpoint_id"
	ProviderKey    = "provider"
)

type QueueManager struct {
	client *redis.Client
}

func NewQueueManager(client *redis.Client) *QueueManager {
	return &QueueManager{client: client}
}

// InitConsumerGroup ينشئ مجموعة المستهلكين إذا لم تكن موجودة مسبقاً
func (q *QueueManager) InitConsumerGroup(ctx context.Context) error {
	// أمر XGroupCreateMkStream ينشئ الرتل ومجموعة المستهلكين ذرّياً
	err := q.client.XGroupCreateMkStream(ctx, StreamName, ConsumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return fmt.Errorf("failed to create consumer group: %w", err)
	}
	return nil
}

// PublishEvent ينشر معرف الحدث في الرتل
func (q *QueueManager) PublishEvent(ctx context.Context, eventID, endpointID, provider string) error {
	args := &redis.XAddArgs{
		Stream: StreamName,
		Values: map[string]interface{}{
			EventIDKey:    eventID,
			EndpointIDKey: endpointID,
			ProviderKey:   provider,
		},
	}
	return q.client.XAdd(ctx, args).Err()
}

// FetchMessages يسحب دفعة من الرسائل الموجهة لمستهلك محدد
func (q *QueueManager) FetchMessages(ctx context.Context, consumerName string, count int64) ([]redis.XMessage, error) {
	streams, err := q.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: consumerName,
		Streams:  []string{StreamName, ">"}, // العلامة > تعني الرسائل الجديدة فقط
		Count:    count,
		Block:    2 * time.Second, // تعليق الاتصال لمدة ثانيتين بانتظار أحداث جديدة (Long Polling)
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}

	if len(streams) == 0 {
		return nil, nil
	}

	return streams[0].Messages, nil
}

// AckMessage يرسل إشعار تأكيد للريديس بأن الرسالة عولجت بنجاح لحذفها من قائمة المعلقات
func (q *QueueManager) AckMessage(ctx context.Context, messageID string) error {
	return q.client.XAck(ctx, StreamName, ConsumerGroup, messageID).Err()
}
