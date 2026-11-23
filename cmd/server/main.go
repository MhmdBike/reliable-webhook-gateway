package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/MhmdBike/reliable-webhook-gateway/internal/config"
	"github.com/MhmdBike/reliable-webhook-gateway/internal/queue"
	"github.com/MhmdBike/reliable-webhook-gateway/internal/verifier"
)

type Server struct {
	cfg    *config.Config
	db     *pgxpool.Pool
	redis  *redis.Client
	queue  *queue.QueueManager
	router *chi.Mux
}

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("فشل تحميل الإعدادات: %v", err)
	}

	ctx := context.Background()

	dbPool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("خطأ في الاتصال بقاعدة البيانات: %v", err)
	}
	defer dbPool.Close()

	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
	})
	defer rdb.Close()

	qMgr := queue.NewQueueManager(rdb)
	if err := qMgr.InitConsumerGroup(ctx); err != nil {
		log.Fatalf("خطأ في تهيئة رتل الرسائل: %v", err)
	}

	srv := &Server{
		cfg:    cfg,
		db:     dbPool,
		redis:  rdb,
		queue:  qMgr,
		router: chi.NewRouter(),
	}

	srv.setupRoutes()

	fmt.Printf(" بوابة الـ Webhooks تعمل على المنفذ %s...\n", cfg.ServerPort)
	if err := http.ListenAndServe(":"+cfg.ServerPort, srv.router); err != nil {
		log.Fatalf("توقف الخادم: %v", err)
	}
}

func (s *Server) setupRoutes() {
	s.router.Use(middleware.RequestID)
	s.router.Use(middleware.Logger)
	s.router.Use(middleware.Recoverer)

	s.router.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"UP"}`))
	})

	// مسار الاستقبال الخارجي
	s.router.Post("/api/v1/webhooks/{provider}", s.handleWebhookIngest)

	// مسارات إدارة ومراقبة الرسائل الميتة (DLQ Management)
	s.router.Get("/api/v1/dlq", s.handleGetDLQ)
	s.router.Get("/api/v1/events/{id}/attempts", s.handleGetAttempts)
	s.router.Post("/api/v1/dlq/{id}/replay", s.handleReplayDLQ)
}

func (s *Server) handleWebhookIngest(w http.ResponseWriter, r *http.Request) {
	provider := chi.URLParam(r, "provider")
	signature := r.Header.Get("X-Signature-SHA256")
	idempotencyKey := r.Header.Get("X-Idempotency-Key")

	if idempotencyKey == "" {
		http.Error(w, `{"error":"Missing X-Idempotency-Key header"}`, http.StatusBadRequest)
		return
	}

	payloadBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"Failed to read request body"}`, http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	if err := verifier.VerifyHMACSHA256(payloadBytes, signature, s.cfg.GatewaySecret); err != nil {
		http.Error(w, `{"error":"Unauthorized: `+err.Error()+`"}`, http.StatusUnauthorized)
		return
	}

	ctx := r.Context()
	lockKey := fmt.Sprintf("idempotency:%s:%s", provider, idempotencyKey)
	isNew, err := s.redis.SetNX(ctx, lockKey, "PROCESSING", 24*time.Hour).Result()
	if err != nil {
		http.Error(w, `{"error":"Internal state error"}`, http.StatusInternalServerError)
		return
	}

	if !isNew {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"already_processed"}`))
		return
	}

	headersJSON, _ := json.Marshal(r.Header)

	query := `
		INSERT INTO webhook_events (idempotency_key, provider, headers, payload, status)
		VALUES ($1, $2, $3, $4, 'PENDING')
		RETURNING id;
	`
	var eventID string
	err = s.db.QueryRow(ctx, query, idempotencyKey, provider, headersJSON, payloadBytes).Scan(&eventID)
	if err != nil {
		s.redis.Del(ctx, lockKey)
		http.Error(w, `{"error":"Failed to persist event"}`, http.StatusInternalServerError)
		return
	}

	_ = s.queue.PublishEvent(ctx, eventID, "", provider)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"message":  "Webhook accepted and queued for delivery",
		"event_id": eventID,
	})
}

// handleGetDLQ يجلب قائمة بكافة الأحداث الميتة المحفوظة في النظام
func (s *Server) handleGetDLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	query := `
		SELECT id, provider, idempotency_key, retry_count, max_retries, created_at 
		FROM webhook_events 
		WHERE status = 'DEAD' 
		ORDER BY created_at DESC 
		LIMIT 50;
	`
	rows, err := s.db.Query(ctx, query)
	if err != nil {
		http.Error(w, `{"error":"Database query failed"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type DeadEvent struct {
		ID             string    `json:"id"`
		Provider       string    `json:"provider"`
		IdempotencyKey string    `json:"idempotency_key"`
		RetryCount     int       `json:"retry_count"`
		MaxRetries     int       `json:"max_retries"`
		CreatedAt      time.Time `json:"created_at"`
	}

	var events []DeadEvent
	for rows.Next() {
		var e DeadEvent
		if err := rows.Scan(&e.ID, &e.Provider, &e.IdempotencyKey, &e.RetryCount, &e.MaxRetries, &e.CreatedAt); err == nil {
			events = append(events, e)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(events)
}

// handleGetAttempts يجلب سجل المحاولات التفصيلي لحدث معين
func (s *Server) handleGetAttempts(w http.ResponseWriter, r *http.Request) {
	eventID := chi.URLParam(r, "id")
	ctx := r.Context()

	query := `
		SELECT attempt_number, status_code, execution_time_ms, COALESCE(error_message, ''), created_at 
		FROM webhook_delivery_attempts 
		WHERE event_id = $1 
		ORDER BY attempt_number ASC;
	`
	rows, err := s.db.Query(ctx, query, eventID)
	if err != nil {
		http.Error(w, `{"error":"Failed to fetch attempts"}`, http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Attempt struct {
		AttemptNumber   int       `json:"attempt_number"`
		StatusCode      int       `json:"status_code"`
		ExecutionTimeMs int       `json:"execution_time_ms"`
		ErrorMessage    string    `json:"error_message"`
		CreatedAt       time.Time `json:"created_at"`
	}

	var attempts []Attempt
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.AttemptNumber, &a.StatusCode, &a.ExecutionTimeMs, &a.ErrorMessage, &a.CreatedAt); err == nil {
			attempts = append(attempts, a)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(attempts)
}

// handleReplayDLQ يعيد إحياء رسالة ميتة وضخها في الرتل للتسليم
func (s *Server) handleReplayDLQ(w http.ResponseWriter, r *http.Request) {
	eventID := chi.URLParam(r, "id")
	ctx := r.Context()

	// تحديث الحالة وتصفير المحاولات ذرّياً
	query := `
		UPDATE webhook_events 
		SET status = 'PENDING', retry_count = 0, next_retry_at = NOW() 
		WHERE id = $1 AND status = 'DEAD' 
		RETURNING provider;
	`
	var provider string
	err := s.db.QueryRow(ctx, query, eventID).Scan(&provider)
	if err != nil {
		http.Error(w, `{"error":"Event not found or not in DEAD state"}`, http.StatusNotFound)
		return
	}

	// ضخ المهمة في الرتل مجدداً
	err = s.queue.PublishEvent(ctx, eventID, "", provider)
	if err != nil {
		http.Error(w, `{"error":"Failed to enqueue event"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"message":  "Event replayed successfully and enqueued",
		"event_id": eventID,
	})
}
