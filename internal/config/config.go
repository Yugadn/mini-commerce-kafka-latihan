package config

import (
	"os"
	"strings"
)

func Env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func Brokers() []string {
	return strings.Split(Env("KAFKA_BROKERS", "localhost:9092"), ",")
}
