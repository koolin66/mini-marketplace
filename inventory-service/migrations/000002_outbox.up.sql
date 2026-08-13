CREATE TABLE outbox (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_id UUID NOT NULL,          -- order_id, к которому относится ответное событие
    event_type   VARCHAR(50) NOT NULL,   -- "stock.reserved" / "stock.failed"
    payload      JSONB NOT NULL,
    status       VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at      TIMESTAMPTZ
);

CREATE INDEX idx_outbox_status_created ON outbox (status, created_at) WHERE status = 'PENDING';