-- 1. جدول الوجهات والأنظمة الداخلية
CREATE TABLE IF NOT EXISTS webhook_endpoints (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name VARCHAR(100) NOT NULL,
    target_url TEXT NOT NULL,
    secret_key VARCHAR(255) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. جدول الرسائل والبيانات الخام المستلمة
CREATE TABLE IF NOT EXISTS webhook_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id UUID REFERENCES webhook_endpoints(id) ON DELETE CASCADE,
    idempotency_key VARCHAR(255) NOT NULL,
    provider VARCHAR(50) NOT NULL,
    headers JSONB NOT NULL,
    payload JSONB NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 5,
    next_retry_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. جدول تسجيل محاولات التسليم والردود
CREATE TABLE IF NOT EXISTS webhook_delivery_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID REFERENCES webhook_events(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    status_code INT,
    response_body TEXT,
    execution_time_ms INT NOT NULL,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- الفهارس لتسريع البحث وتفادي التكرار
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_provider_idempotency 
    ON webhook_events (provider, idempotency_key);

CREATE INDEX IF NOT EXISTS idx_events_pending_retry 
    ON webhook_events (status, next_retry_at) 
    WHERE status = 'PENDING';
