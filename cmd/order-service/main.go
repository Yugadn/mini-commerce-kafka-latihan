// order-service: menerima order lewat HTTP lalu publish event OrderCreated ke Kafka.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/yugadn/mini-qcommerce/internal/config"
	"github.com/yugadn/mini-qcommerce/internal/event"
)

type createOrderRequest struct {
	UserID string            `json:"user_id"`
	Items  []event.OrderItem `json:"items"`
}

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	writer := &kafka.Writer{
		Addr:  kafka.TCP(config.Brokers()...),
		Topic: event.TopicOrders,
		// Hash balancer: key yang sama -> partition yang sama -> urutan terjaga per order.
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll, // tunggu semua replica in-sync
		BatchTimeout: 10 * time.Millisecond,
	}
	defer writer.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, r *http.Request) {
		var req createOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Items) == 0 {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		for _, it := range req.Items {
			if it.ProductID == "" || it.Quantity <= 0 {
				http.Error(w, `{"error":"invalid item"}`, http.StatusBadRequest)
				return
			}
		}

		evt := event.OrderCreated{
			EventID:    uuid.NewString(),
			EventType:  event.TypeOrderCreated,
			OrderID:    "ORD-" + uuid.NewString()[:8],
			UserID:     req.UserID,
			Items:      req.Items,
			OccurredAt: time.Now().UTC(),
		}
		payload, _ := json.Marshal(evt)

		// TODO(minggu 6): ganti publish langsung ini dengan outbox pattern,
		// supaya simpan order ke DB dan publish event tidak bisa "setengah jadi".
		err := writer.WriteMessages(r.Context(), kafka.Message{
			Key:   []byte(evt.OrderID),
			Value: payload,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(evt.EventType)},
			},
		})
		if err != nil {
			log.Error("publish failed", "order_id", evt.OrderID, "err", err)
			http.Error(w, `{"error":"failed to publish"}`, http.StatusServiceUnavailable)
			return
		}

		log.Info("order created", "order_id", evt.OrderID, "event_id", evt.EventID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"order_id": evt.OrderID, "event_id": evt.EventID})
	})

	srv := &http.Server{Addr: config.Env("HTTP_ADDR", ":8080"), Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		log.Info("order-service listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	log.Info("shutting down order-service")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
}
