package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	ServerPort    string
	ServerEnv     string
	DatabaseURL   string
	RedisAddr     string
	RedisPassword string
	GatewaySecret string
}

func LoadConfig() (*Config, error) {
	_ = godotenv.Load()

	dbHost := getEnv("DB_HOST", "localhost")
	dbPort := getEnv("DB_PORT", "5432")
	dbUser := getEnv("DB_USER", "gateway_user")
	dbPass := getEnv("DB_PASSWORD", "gateway_password")
	dbName := getEnv("DB_NAME", "webhook_gateway")
	dbSSL := getEnv("DB_SSLMODE", "disable")

	dbURL := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=%s",
		dbUser, dbPass, dbHost, dbPort, dbName, dbSSL)

	cfg := &Config{
		ServerPort:    getEnv("SERVER_PORT", "8080"),
		ServerEnv:     getEnv("SERVER_ENV", "development"),
		DatabaseURL:   dbURL,
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		GatewaySecret: getEnv("GATEWAY_SECRET", "default_secret"),
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return fallback
}
