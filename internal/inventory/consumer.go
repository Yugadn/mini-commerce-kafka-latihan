package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/yugadn/mini-qcommerce/internal/event"
)

const maxAttempts = 3

type Consumer struct {
	reader *kafka.Reader
	dlq    *kafka.Writer
	repo   *Repository
	log    *slog.Logger
}

func NewConsumer(reader *kafka.Reader, dlq *kafka.Writer, repo *Repository, log *slog.Logger) *Consumer {
	return &Consumer{reader: reader, dlq: dlq, repo: repo, log: log}
}

// Run membaca message satu per satu. Offset di-commit MANUAL setelah message
// selesai diproses (sukses, duplikat, atau sudah masuk DLQ) -> at-least-once.
func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch: %w", err)
		}

		log := c.log.With("partition", msg.Partition, "offset", msg.Offset, "key", string(msg.Key))

		if err := c.handleWithRetry(ctx, msg, log); err != nil {
			log.Error("sending to DLQ", "err", err)
			if dlqErr := c.sendToDLQ(ctx, msg, err); dlqErr != nil {
				// Jangan commit: message akan dibaca ulang setelah restart.
				return fmt.Errorf("dlq publish failed: %w", dlqErr)
			}
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
	}
}

func (c *Consumer) handleWithRetry(ctx context.Context, msg kafka.Message, log *slog.Logger) error {
	var evt event.OrderCreated
	if err := json.Unmarshal(msg.Value, &evt); err != nil {
		return fmt.Errorf("invalid payload: %w", err) // tidak perlu retry
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		dup, err := c.repo.ApplyOrder(ctx, evt)
		if err == nil {
			if dup {
				log.Info("duplicate event skipped", "event_id", evt.EventID)
			} else {
				log.Info("stock reserved", "order_id", evt.OrderID, "event_id", evt.EventID)
			}
			return nil
		}
		if errors.Is(err, ErrInsufficientStock) {
			return err // error bisnis: retry percuma
		}

		lastErr = err
		backoff := time.Duration(1<<(attempt-1)) * 200 * time.Millisecond // 200ms, 400ms, 800ms
		log.Warn("process failed, retrying", "attempt", attempt, "backoff", backoff, "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return fmt.Errorf("after %d attempts: %w", maxAttempts, lastErr)
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, cause error) error {
	headers := append([]kafka.Header{}, msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "dlq_error", Value: []byte(cause.Error())},
		kafka.Header{Key: "dlq_source_partition", Value: []byte(fmt.Sprint(msg.Partition))},
		kafka.Header{Key: "dlq_source_offset", Value: []byte(fmt.Sprint(msg.Offset))},
	)
	return c.dlq.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
}
