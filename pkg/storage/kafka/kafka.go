package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/Shopify/sarama"
	"github.com/profefe/profefe/pkg/log"
	"github.com/profefe/profefe/pkg/profile"
	"github.com/profefe/profefe/pkg/storage"
	"go.uber.org/zap"
)

type Writer struct {
	logger   *log.Logger
	producer sarama.AsyncProducer
	topic    string
}

var _ storage.Writer = (*Writer)(nil)

func New(logger *log.Logger, producer sarama.AsyncProducer, topic string) *Writer {
	go func() {
		for err := range producer.Errors() {
			logger.Errorw("kafka writer", zap.Error(err))
		}
	}()

	return &Writer{
		logger:   logger,
		producer: producer,
		topic:    topic,
	}
}

func (w *Writer) WriteProfile(ctx context.Context, params *storage.WriteProfileParams, _ io.Reader) (profile.Meta, error) {
	createdAt := params.CreatedAt
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}

	meta := profile.Meta{
		ProfileID:  profile.TestID,
		ExternalID: params.ExternalID,
		Service:    params.Service,
		Type:       params.Type,
		Labels:     params.Labels,
		CreatedAt:  createdAt,
	}

	value, err := json.Marshal(meta)
	if err != nil {
		return profile.Meta{}, fmt.Errorf("could not encode profile meta %v: %w", meta, err)
	}

	message := &sarama.ProducerMessage{
		Topic: w.topic,
		Value: sarama.ByteEncoder(value),
	}
	select {
	case w.producer.Input() <- message:
	case <-ctx.Done():
		return profile.Meta{}, ctx.Err()
	}

	w.logger.Debugw("writeProfile: kafka produce", "pid", meta.ProfileID, "topic", message.Topic, "value", meta)

	return meta, nil
}
