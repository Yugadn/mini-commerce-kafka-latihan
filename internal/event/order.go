// Package event berisi kontrak event yang dibagi antar service.
package event

import "time"

const (
	TopicOrders    = "orders"
	TopicOrdersDLQ = "orders-dlq"

	TypeOrderCreated = "OrderCreated"
)

type OrderItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

// OrderCreated dipublish oleh order-service setiap ada order baru.
// EventID dipakai consumer untuk idempotency; OrderID dipakai sebagai
// message key supaya semua event satu order masuk partition yang sama (ordering).
type OrderCreated struct {
	EventID    string      `json:"event_id"`
	EventType  string      `json:"event_type"`
	OrderID    string      `json:"order_id"`
	UserID     string      `json:"user_id"`
	Items      []OrderItem `json:"items"`
	OccurredAt time.Time   `json:"occurred_at"`
}
