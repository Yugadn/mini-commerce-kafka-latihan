.PHONY: up down deps order inventory order-test dlq stock reset

up:            ## Jalankan Kafka, Kafka UI, Postgres
	docker compose up -d

down:          ## Matikan semua container + hapus volume
	docker compose down -v

deps:
	go mod tidy

order:         ## Jalankan order-service (port 8080)
	go run ./cmd/order-service

inventory:     ## Jalankan inventory-service (consumer)
	go run ./cmd/inventory-service

order-test:    ## Kirim satu order contoh
	curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
	  -d '{"user_id":"U-1","items":[{"product_id":"SKU-SUSU-1L","quantity":2},{"product_id":"SKU-TELUR-10","quantity":1}]}'; echo

dlq:           ## Lihat isi topic DLQ
	docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
	  --topic orders.dlq --from-beginning --property print.headers=true --timeout-ms 5000

stock:         ## Lihat stok saat ini
	docker exec -it postgres psql -U app -d inventory -c "SELECT product_id, quantity FROM stocks ORDER BY product_id;"

reset:         ## Kembalikan stok & hapus riwayat event
	docker exec -it postgres psql -U app -d inventory -c "TRUNCATE processed_events; UPDATE stocks SET quantity = CASE product_id WHEN 'SKU-SUSU-1L' THEN 100 WHEN 'SKU-TELUR-10' THEN 50 WHEN 'SKU-ROTI-TAWAR' THEN 30 ELSE 200 END;"
