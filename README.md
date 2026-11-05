# mini-qcommerce — Minggu 1: Kafka

Project latihan untuk roadmap 6 minggu. Minggu ini: `order-service` publish event `OrderCreated` ke Kafka, `inventory-service` consume dan mengurangi stok di PostgreSQL dengan aman (idempotent, retry, DLQ).

```
POST /orders ──► order-service ──(topic: orders, key=order_id, 3 partition)──► inventory-service ──► PostgreSQL
                                                                                      │
                                                                     gagal 3x / stok kurang
                                                                                      ▼
                                                                              topic: orders.dlq
```

## Struktur

```
cmd/
  order-service/main.go       HTTP API + Kafka producer
  inventory-service/main.go   Kafka consumer + wiring DB
internal/
  event/order.go              Kontrak event (dipakai kedua service)
  inventory/consumer.go       Fetch → proses + retry → DLQ → commit offset
  inventory/repository.go     Update stok + idempotency dalam 1 transaksi
  config/config.go            Baca env var
migrations/001_init.sql       Tabel stocks, processed_events + seed produk
scripts/burst.sh              Kirim banyak order sekaligus
docker-compose.yml            Kafka (KRaft), Kafka UI, Postgres
```

## Menjalankan

Butuh Go 1.24+ dan Docker.

```bash
make deps        # go mod tidy (sekali saja)
make up          # Kafka, Kafka UI (http://localhost:8090), Postgres
make inventory   # terminal 1
make order       # terminal 2
make order-test  # terminal 3 → kirim 1 order
make stock       # cek stok berkurang
```

## Keputusan desain yang perlu kamu pahami

| Keputusan | Kenapa |
| --- | --- |
| Key message = `order_id` | Event satu order selalu ke partition yang sama → urutan terjaga |
| `RequiredAcks: RequireAll` | Event tidak hilang walau leader broker mati (penting di cluster multi-broker) |
| `FetchMessage` + `CommitMessages` manual | Offset baru di-commit setelah selesai diproses → at-least-once |
| Tabel `processed_events` di transaksi yang sama | At-least-once berarti bisa duplikat; ini yang membuatnya efektif exactly-once |
| `UPDATE ... WHERE quantity >= $1` | Mencegah stok negatif tanpa race condition |
| Stok kurang langsung ke DLQ | Error bisnis tidak akan sembuh dengan retry |
| Error DB di-retry 3x (200/400/800 ms) | Error sementara (koneksi putus) biasanya pulih |
| DLQ gagal → tidak commit, service berhenti | Lebih baik berhenti daripada kehilangan message |

## Latihan minggu 1

- [ ] **Jalan normal**: `make order-test`, lihat log kedua service dan message di Kafka UI
- [ ] **Consumer mati**: stop `inventory-service`, jalankan `./scripts/burst.sh 10`, nyalakan lagi. Stok `SKU-AIR-600` harus turun tepat 10
- [ ] **Duplikat**: di Kafka UI, reset offset group `inventory-service` ke awal, restart consumer. Log harus menampilkan `duplicate event skipped` dan stok tidak berubah
- [ ] **Stok kurang**: order `SKU-ROTI-TAWAR` dengan quantity 999, lalu `make dlq`. Lihat header `dlq_error`
- [ ] **Error sementara**: matikan Postgres (`docker stop postgres`) saat ada order masuk, amati retry di log, lalu nyalakan lagi
- [ ] **Consumer group**: jalankan 3 instance `inventory-service`, lihat pembagian partition di Kafka UI (tab Consumers). Jalankan instance ke-4 dan lihat kenapa ia idle
- [ ] **Ordering**: tambahkan event `OrderCancelled` dengan key `order_id` yang sama, pastikan diproses setelah `OrderCreated`

## Tantangan tambahan (opsional)

- [ ] Ganti `processed_events` menjadi `reservations` (order_id, product_id, qty) supaya `OrderCancelled` bisa mengembalikan stok
- [ ] Tambahkan tool kecil untuk me-replay message dari `orders.dlq` ke `orders`
- [ ] Bandingkan: apa yang terjadi kalau `CommitMessages` dipanggil SEBELUM `ApplyOrder`? (at-most-once)

## Pertanyaan interview yang harus bisa kamu jawab setelah minggu ini

1. Bagaimana Kafka menjamin ordering, dan kapan ordering bisa rusak?
2. Apa beda at-least-once dan exactly-once, dan bagaimana kamu mencapainya di project ini?
3. Apa yang terjadi saat consumer group rebalance di tengah pemrosesan?
4. Kapan memilih Kafka dibanding RabbitMQ?
5. Bagaimana menangani poison message?
