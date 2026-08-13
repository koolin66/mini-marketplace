CREATE TABLE orders (
    id           UUID PRIMARY KEY,
    customer_id  UUID NOT NULL,
    total_amount BIGINT NOT NULL,      -- деньги в минимальных единицах, не float
    currency     VARCHAR(3) NOT NULL,
    status       VARCHAR(20) NOT NULL,
    version      INT NOT NULL DEFAULT 1,
    created_at   TIMESTAMPTZ NOT NULL,
    updated_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE order_items (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    order_id    UUID NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    sku         VARCHAR(64) NOT NULL,
    name        VARCHAR(255) NOT NULL,
    price       BIGINT NOT NULL,
    currency    VARCHAR(3) NOT NULL,
    quantity    INT NOT NULL
);

-- Индекс под курсорную пагинацию: сортировка по (created_at, id) для конкретного customer_id.
CREATE INDEX idx_orders_customer_cursor ON orders (customer_id, created_at DESC, id DESC);