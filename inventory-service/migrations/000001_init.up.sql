CREATE TABLE stock (
    sku            VARCHAR(64) PRIMARY KEY,
    available_qty  INT NOT NULL CHECK (available_qty >= 0),
    reserved_qty   INT NOT NULL DEFAULT 0 CHECK (reserved_qty >= 0),
    version        INT NOT NULL DEFAULT 1,
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Таблица для идемпотентности: какие order_id уже обработаны, чтобы дубликат
-- события order.created (at-least-once доставка из Kafka) не зарезервировал товар дважды.
CREATE TABLE processed_events (
    order_id     UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);