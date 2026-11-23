package dispatcher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Dispatcher struct {
	db         *pgxpool.Pool
	httpClient *http.Client
}

func NewDispatcher(db *pgxpool.Pool) *Dispatcher {
	return &Dispatcher{
		db: db,
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // مهلة الاتصال القصوى لكل محاولة
		},
	}
}

// ProcessEvent ينفذ محاولة إرسال الـ Webhook ويحدث الحالة في قاعدة البيانات
func (d *Dispatcher) ProcessEvent(ctx context.Context, eventID string, defaultTargetURL string) error {
	// 1. قراءة بيانات الحدث من PostgreSQL
	var payload []byte
	var retryCount, maxRetries int
	var currentStatus string

	query := `
		SELECT payload, retry_count, max_retries, status 
		FROM webhook_events 
		WHERE id = $1
	`
	err := d.db.QueryRow(ctx, query, eventID).Scan(&payload, &retryCount, &maxRetries, &currentStatus)
	if err != nil {
		return fmt.Errorf("failed to load event %s: %w", eventID, err)
	}

	if currentStatus == "DELIVERED" {
		return nil // تمت معالجته مسبقاً
	}

	// 2. محاولة التسليم عبر HTTP POST
	startTime := time.Now()
	req, err := http.NewRequestWithContext(ctx, "POST", defaultTargetURL, bytes.NewBuffer(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Reliable-Webhook-Gateway/1.0")

	resp, err := d.httpClient.Do(req)
	executionTime := time.Since(startTime).Milliseconds()

	var statusCode int
	var respBody string
	var attemptErr string

	if err != nil {
		attemptErr = err.Error()
	} else {
		statusCode = resp.StatusCode
		bodyBytes, _ := io.ReadAll(resp.Body)
		respBody = string(bodyBytes)
		resp.Body.Close()
	}

	// 3. تدوين المحاولة في جدول webhook_delivery_attempts
	logQuery := `
		INSERT INTO webhook_delivery_attempts (event_id, attempt_number, status_code, response_body, execution_time_ms, error_message)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	_, _ = d.db.Exec(ctx, logQuery, eventID, retryCount+1, statusCode, respBody, executionTime, attemptErr)

	// 4. فحص النتيجة: نجاح (2xx) أم فشل؟
	isSuccess := err == nil && statusCode >= 200 && statusCode < 300

	if isSuccess {
		_, err = d.db.Exec(ctx, `UPDATE webhook_events SET status = 'DELIVERED', updated_at = NOW() WHERE id = $1`, eventID)
		fmt.Printf(" [نجاح] تم تسليم الحدث %s بنجاح في %dms (Status: %d)\n", eventID, executionTime, statusCode)
		return nil
	}

	// في حال الفشل:
	newRetryCount := retryCount + 1
	if newRetryCount >= maxRetries {
		// استنفاد جميع المحاولات -> نقل إلى حالة DEAD
		_, _ = d.db.Exec(ctx, `UPDATE webhook_events SET status = 'DEAD', retry_count = $2 WHERE id = $1`, eventID, newRetryCount)
		fmt.Printf(" [استنفاد] فشل تسليم الحدث %s بعد %d محاولات -> تحويل إلى DEAD\n", eventID, newRetryCount)
		return nil
	}

	// حساب التراجع الأسي مع التذبذب العشوائي (Exponential Backoff + Jitter)
	backoffSec := math.Min(3600, 5*math.Pow(2, float64(newRetryCount)))
	jitter := backoffSec * (0.8 + rand.Float64()*0.4) // تذبذب بمعدل ±20%
	nextRetry := time.Now().Add(time.Duration(jitter) * time.Second)

	_, _ = d.db.Exec(ctx, `
		UPDATE webhook_events 
		SET status = 'PENDING', retry_count = $2, next_retry_at = $3 
		WHERE id = $1
	`, eventID, newRetryCount, nextRetry)

	fmt.Printf(" [إعادة محاولة] فشل تسليم %s. المحاولة القادمة رقم %d بعد %.1f ثانية\n", eventID, newRetryCount, jitter)
	return nil
}
