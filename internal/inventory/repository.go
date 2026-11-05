package inventory

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/yugadn/mini-qcommerce/internal/event"
)

// ErrInsufficientStock adalah error bisnis: retry tidak akan menolong,
// jadi consumer langsung mengirimnya ke DLQ.
var ErrInsufficientStock = errors.New("insufficient stock")

type Repository struct{ db *sql.DB }

func NewRepository(db *sql.DB) *Repository { return &Repository{db: db} }

// ApplyOrder mengurangi stok untuk semua item dalam satu transaksi.
// Idempotent: kalau event_id sudah pernah diproses, tidak ada yang berubah
// dan alreadyProcessed = true.
func (r *Repository) ApplyOrder(ctx context.Context, evt event.OrderCreated) (alreadyProcessed bool, err error) {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op setelah Commit

	res, err := tx.ExecContext(ctx,
		`INSERT INTO processed_events (event_id, order_id) VALUES ($1, $2) ON CONFLICT (event_id) DO NOTHING`,
		evt.EventID, evt.OrderID)
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return true, nil // duplikat: sudah diproses sebelumnya
	}

	for _, it := range evt.Items {
		// Kondisi quantity >= $1 mencegah stok negatif tanpa SELECT terpisah.
		res, err := tx.ExecContext(ctx,
			`UPDATE stocks SET quantity = quantity - $1, updated_at = now()
			 WHERE product_id = $2 AND quantity >= $1`,
			it.Quantity, it.ProductID)
		if err != nil {
			return false, err
		}
		if n, _ := res.RowsAffected(); n == 0 {
			return false, fmt.Errorf("%w: %s", ErrInsufficientStock, it.ProductID)
		}
	}

	return false, tx.Commit()
}
