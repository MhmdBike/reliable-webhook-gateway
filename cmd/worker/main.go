package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"

	"github.com/MhmdBike/reliable-webhook-gateway/internal/config"
	"github.com/MhmdBike/reliable-webhook-gateway/internal/dispatcher"
	"github.com/MhmdBike/reliable-webhook-gateway/internal/queue"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("فشل تحميل الإعدادات: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. الاتصال بقواعد البيانات
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
		log.Fatalf("فشل تهيئة مجموعة المستهلكين في Redis: %v", err)
	}

	disp := dispatcher.NewDispatcher(dbPool)

	consumerName := fmt.Sprintf("worker-%d", os.Getpid())
	fmt.Printf(" العامل الخلفي [%s] يعمل الآن ويراقب الرتل...\n", consumerName)

	// رصد إشارات الإيقاف الآمن (Graceful Shutdown)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		fmt.Println("\n تم استقبال إشارة إيقاف... إغلاق العامل بأمان.")
		cancel()
	}()

	// 2. تشغيل مجدول إعادة المحاولات في Goroutine منفصلة
	go startRetryScheduler(ctx, dbPool, qMgr)

	targetURL := "http://localhost:9000/webhook-receiver"

	// 3. حلقة الاستماع وسحب المهام من Redis Streams
	for {
		select {
		case <-ctx.Done():
			fmt.Println(" تم إيقاف حلقة عمل الـ Worker.")
			return
		default:
			messages, err := qMgr.FetchMessages(ctx, consumerName, 1)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("خطأ أثناء سحب الرسائل: %v", err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			for _, msg := range messages {
				eventID, ok := msg.Values[queue.EventIDKey].(string)
				if !ok {
					continue
				}

				fmt.Printf("⚡ [Worker] معالجة الحدث %s من الرتل...\n", eventID)
				_ = disp.ProcessEvent(ctx, eventID, targetURL)

				// تأكيد معالجة الرسالة (ACK)
				_ = qMgr.AckMessage(ctx, msg.ID)
			}
		}
	}
}

// startRetryScheduler يفحص قاعدة البيانات دورياً للمهام المعلقة المستحقة
func startRetryScheduler(ctx context.Context, db *pgxpool.Pool, qMgr *queue.QueueManager) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	fmt.Println(" [Scheduler] تم تشغيل مجدول إعادة المحاولات التلقائي (فحص كل 5 ثوانٍ)...")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// استعلام يسحب العمليات التي حان موعدها مع قفل الأسطر لتفادي التكرار
			query := `
				WITH claimable AS (
					SELECT id, provider 
					FROM webhook_events 
					WHERE status = 'PENDING' 
					  AND next_retry_at <= NOW() 
					  AND retry_count > 0
					ORDER BY next_retry_at ASC 
					LIMIT 20
					FOR UPDATE SKIP LOCKED
				)
				UPDATE webhook_events e
				SET next_retry_at = NOW() + INTERVAL '1 minute'
				FROM claimable c
				WHERE e.id = c.id
				RETURNING e.id, e.provider;
			`
			rows, err := db.Query(ctx, query)
			if err != nil {
				if ctx.Err() == nil {
					log.Printf("خطأ في فحص مجدول الإعادة: %v", err)
				}
				continue
			}

			for rows.Next() {
				var eventID, provider string
				if err := rows.Scan(&eventID, &provider); err == nil {
					fmt.Printf(" [Scheduler] إعادة جدولة الحدث المستحق %s وضخه في الرتل مجدداً\n", eventID)
					_ = qMgr.PublishEvent(ctx, eventID, "", provider)
				}
			}
			rows.Close()
		}
	}
}
