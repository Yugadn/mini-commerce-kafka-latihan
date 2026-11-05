#!/usr/bin/env bash
# Kirim N order sekaligus. Dipakai untuk latihan:
#   1) matikan inventory-service, 2) jalankan script ini, 3) nyalakan lagi,
#   4) cek `make stock` -> semua order harus terproses tepat sekali.
N=${1:-10}
for i in $(seq 1 "$N"); do
  curl -s -X POST localhost:8080/orders -H 'Content-Type: application/json' \
    -d '{"user_id":"U-'"$i"'","items":[{"product_id":"SKU-AIR-600","quantity":1}]}' &
done
wait
echo
echo "Terkirim $N order. Stok SKU-AIR-600 seharusnya berkurang $N."
