CREATE TABLE outbox (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,          -- order.id, к которому относится событие
    event_type   VARCHAR(50) NOT NULL,   -- "order.created", позже "order.cancelled" и т.д.
    payload      JSONB NOT NULL,         -- сериализованное событие целиком
    status       VARCHAR(20) NOT NULL DEFAULT 'PENDING', -- PENDING / SENT / FAILED
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at      TIMESTAMPTZ            -- NULL пока не отправлено воркером
);

-- Индекс под воркер: он постоянно ищет "SELECT * FROM outbox WHERE status = 'PENDING'".
CREATE INDEX idx_outbox_status_created ON outbox (status, created_at) WHERE status = 'PENDING';