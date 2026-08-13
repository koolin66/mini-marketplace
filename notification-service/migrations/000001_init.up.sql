CREATE TABLE inbox_events (
    order_id     UUID NOT NULL,
    event_type   VARCHAR(50) NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, event_type)  -- составной ключ: уникальна КОМБИНАЦИЯ заказа И типа события
);