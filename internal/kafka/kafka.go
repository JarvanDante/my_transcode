package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"my_transcode/internal/config"
	"my_transcode/internal/protocol"
)

// Bus Kafka 生产/消费。
type Bus struct {
	cfg    config.KafkaConfig
	writer *kafkago.Writer
}

func New(cfg config.KafkaConfig) *Bus {
	b := &Bus{cfg: cfg}
	if !cfg.Enabled {
		return b
	}
	topic := cfg.ResultTopic
	if topic == "" {
		topic = protocol.TopicResults
	}
	b.writer = &kafkago.Writer{
		Addr:                   kafkago.TCP(cfg.Brokers...),
		Topic:                  topic,
		Balancer:               &kafkago.Hash{},
		RequiredAcks:           kafkago.RequireOne,
		Async:                  false,
		AllowAutoTopicCreation: true,
	}
	return b
}

func (b *Bus) Enabled() bool { return b.cfg.Enabled }

func (b *Bus) Close() error {
	if b.writer != nil {
		return b.writer.Close()
	}
	return nil
}

// EnsureTopics 尽量创建 jobs/results topic（已存在则忽略）。
func (b *Bus) EnsureTopics(ctx context.Context) error {
	if !b.cfg.Enabled {
		return nil
	}
	if len(b.cfg.Brokers) == 0 {
		return fmt.Errorf("kafka brokers empty")
	}
	jobs := b.cfg.JobTopic
	if jobs == "" {
		jobs = protocol.TopicJobs
	}
	results := b.cfg.ResultTopic
	if results == "" {
		results = protocol.TopicResults
	}
	for _, topic := range []string{jobs, results} {
		if err := createTopic(ctx, b.cfg.Brokers[0], topic); err != nil {
			return fmt.Errorf("ensure topic %s: %w", topic, err)
		}
	}
	return nil
}

func createTopic(ctx context.Context, broker, topic string) error {
	conn, err := kafkago.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlAddr := net.JoinHostPort(controller.Host, fmt.Sprintf("%d", controller.Port))
	ctrl, err := kafkago.DialContext(ctx, "tcp", ctrlAddr)
	if err != nil {
		return err
	}
	defer ctrl.Close()

	err = ctrl.CreateTopics(kafkago.TopicConfig{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	})
	if err != nil && !strings.Contains(strings.ToLower(err.Error()), "already exists") {
		// kafka-go may return TopicAlreadyExists
		if strings.Contains(err.Error(), "Topic with this name already exists") {
			return nil
		}
		return err
	}
	return nil
}

// Ping 探测 broker 是否可连（取第一个 broker 建连）。
func (b *Bus) Ping(ctx context.Context) error {
	if !b.cfg.Enabled {
		return fmt.Errorf("kafka disabled")
	}
	if len(b.cfg.Brokers) == 0 {
		return fmt.Errorf("kafka brokers empty")
	}
	d := net.Dialer{Timeout: 3 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", b.cfg.Brokers[0])
	if err != nil {
		return err
	}
	_ = conn.Close()
	return nil
}

// PublishResult 发送转码结果。
func (b *Bus) PublishResult(ctx context.Context, msg protocol.ResultMessage) error {
	if !b.cfg.Enabled {
		log.Printf("kafka disabled, skip publish result job_id=%s status=%s", msg.JobID, msg.Status)
		return nil
	}
	if b.writer == nil {
		return fmt.Errorf("kafka writer not initialized")
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return b.writer.WriteMessages(ctx, kafkago.Message{
		Key:   []byte(msg.JobID),
		Value: body,
		Time:  time.Now(),
	})
}

// ConsumeJobs 阻塞消费任务。
func (b *Bus) ConsumeJobs(ctx context.Context, handler func(context.Context, protocol.JobMessage) error) error {
	if !b.cfg.Enabled {
		log.Println("kafka: disabled, skip consumer")
		<-ctx.Done()
		return ctx.Err()
	}

	topic := b.cfg.JobTopic
	if topic == "" {
		topic = protocol.TopicJobs
	}
	group := b.cfg.GroupID
	if group == "" {
		group = "my_transcode"
	}

	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        b.cfg.Brokers,
		GroupID:        group,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: time.Second,
		StartOffset:    kafkago.FirstOffset,
	})
	defer r.Close()

	log.Printf("kafka: consuming topic=%s group=%s brokers=%s", topic, group, strings.Join(b.cfg.Brokers, ","))

	for {
		m, err := r.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("kafka fetch: %w", err)
		}

		var job protocol.JobMessage
		if err := json.Unmarshal(m.Value, &job); err != nil {
			log.Printf("kafka: invalid job json: %v; skip", err)
			_ = r.CommitMessages(ctx, m)
			continue
		}

		if err := handler(ctx, job); err != nil {
			log.Printf("kafka: job handle failed id=%s: %v (commit anyway to avoid poison loop; retry via new job)", job.JobID, err)
		}
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Printf("kafka: commit failed: %v", err)
		}
	}
}
