// inventory-service: consume OrderCreated dari Kafka dan kurangi stok di PostgreSQL.
package main

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/segmentio/kafka-go"

	"github.com/yugadn/mini-qcommerce/internal/config"
	"github.com/yugadn/mini-qcommerce/internal/event"
	"github.com/yugadn/mini-qcommerce/internal/inventory"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	db, err := sql.Open("pgx", config.Env("DATABASE_URL", "postgres://app:app@localhost:5432/inventory?sslmode=disable"))
	if err != nil {
		log.Error("open db", "err", err)
		os.Exit(1)
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.Brokers(),
		Topic:   event.TopicOrders,
		// Semua instance dengan GroupID sama berbagi partition.
		// Coba jalankan 2-3 instance dan lihat rebalancing di log/Kafka UI.
		GroupID:     config.Env("CONSUMER_GROUP", "inventory-service"),
		StartOffset: kafka.FirstOffset, // group baru mulai dari awal topic
		MinBytes:    1,
		MaxBytes:    10e6,
		MaxWait:     500 * time.Millisecond,
		// CommitInterval 0 = commit sinkron saat CommitMessages dipanggil.
	})
	defer reader.Close()

	dlq := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers()...),
		Topic:        event.TopicOrdersDLQ,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
	}
	defer dlq.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	consumer := inventory.NewConsumer(reader, dlq, inventory.NewRepository(db), log)
	log.Info("inventory-service consuming", "topic", event.TopicOrders)
	if err := consumer.Run(ctx); err != nil {
		log.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
	log.Info("inventory-service stopped gracefully")
}
