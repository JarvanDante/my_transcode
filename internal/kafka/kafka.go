package kafka

import (
	"context"
	"fmt"
	"log"

	"my_transcode/internal/config"
	"my_transcode/internal/protocol"
)

// Bus Kafka 生产/消费骨架。第 5 步再接 segmentio/kafka-go 或 franz-go。
type Bus struct {
	cfg config.KafkaConfig
}

func New(cfg config.KafkaConfig) *Bus {
	return &Bus{cfg: cfg}
}

func (b *Bus) Enabled() bool { return b.cfg.Enabled }

// PublishResult 发送转码结果。
func (b *Bus) PublishResult(ctx context.Context, msg protocol.ResultMessage) error {
	_ = ctx
	_ = msg
	return fmt.Errorf("kafka: PublishResult not implemented yet")
}

// ConsumeJobs 阻塞消费任务；handler 返回 error 时由实现决定是否重试。
func (b *Bus) ConsumeJobs(ctx context.Context, handler func(context.Context, protocol.JobMessage) error) error {
	if !b.cfg.Enabled {
		log.Println("kafka: disabled, skip consumer")
		<-ctx.Done()
		return ctx.Err()
	}
	_ = handler
	return fmt.Errorf("kafka: ConsumeJobs not implemented yet")
}
