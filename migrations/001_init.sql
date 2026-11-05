-- Stok per produk
CREATE TABLE IF NOT EXISTS stocks (
    product_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    quantity    INT  NOT NULL CHECK (quantity >= 0),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Idempotency: setiap event_id hanya boleh diproses sekali.
-- Diisi di transaksi yang SAMA dengan update stok.
CREATE TABLE IF NOT EXISTS processed_events (
    event_id      UUID PRIMARY KEY,
    order_id      TEXT NOT NULL,
    processed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO stocks (product_id, name, quantity) VALUES
    ('SKU-SUSU-1L',   'Susu UHT 1L',        100),
    ('SKU-TELUR-10',  'Telur Ayam isi 10',   50),
    ('SKU-ROTI-TAWAR','Roti Tawar',          30),
    ('SKU-AIR-600',   'Air Mineral 600ml',  200)
ON CONFLICT (product_id) DO NOTHING;
