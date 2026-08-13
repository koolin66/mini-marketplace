# Mini-Marketplace — Order Processing System

Учебный pet-проект на Go: микросервисная архитектура для обработки заказов,
построенная для практической отработки Clean Architecture, gRPC, Kafka
(Outbox/Saga), пессимистичных/оптимистичных блокировок, Redis и Docker.

## Архитектура

```
                         ┌──────────────┐
   Client ── HTTP ──────▶│ API Gateway  │
                         │  (gin)       │
                         │  Redis:      │
                         │  - cache     │
                         │  - rate-lim  │
                         └──────┬───────┘
                                │ gRPC
                                ▼
                         ┌──────────────┐        Kafka
                         │Order Service │◀──────────────────┐
                         │ PostgreSQL   │  order.confirmed   │
                         │ Outbox       │  order.cancelled   │
                         └──────┬───────┘                    │
                                │ order.created               │
                                ▼                             │
                         ┌──────────────┐  stock.reserved     │
                         │  Inventory   │  stock.failed  ─────┘
                         │  Service     │
                         │ PostgreSQL   │
                         │ SELECT FOR   │
                         │ UPDATE       │
                         └──────────────┘

                         ┌──────────────┐
                         │ Notification │◀── order.created
                         │  Service     │◀── order.confirmed
                         │ PostgreSQL   │◀── order.cancelled
                         │ (inbox)      │
                         └──────────────┘
```

## Сервисы

### api-gateway
Единственная публичная точка входа. HTTP (gin) → gRPC клиент к Order Service.
- Redis cache-aside для `GET /orders/:id` (TTL 5s)
- Rate limiting — token bucket через Lua-скрипт в Redis
- Собственные DTO, развязанные от внутреннего protobuf-контракта
- Swagger UI

### order-service
Ядро системы. Clean Architecture: `domain / ports / usecase / repository / delivery`.
- PostgreSQL (pgx) — таблицы `orders`, `order_items`, `outbox`
- gRPC-сервер (принимает от Gateway)
- Outbox pattern — транзакционная запись заказа + события, отдельный воркер
  публикует в Kafka с гарантией at-least-once (`SELECT ... FOR UPDATE SKIP LOCKED`)
- Оптимистичная блокировка (`version`) при обновлении статуса
- Курсорная пагинация (`created_at, id`) для списка заказов
- Kafka consumer — слушает `stock.reserved` / `stock.failed`, завершает Saga
- Явная FSM для статусов заказа (`CREATED → AWAITING_STOCK → CONFIRMED/CANCELLED`)

### inventory-service
Резервирование товара. Отдельная база данных (database per service).
- PostgreSQL — таблицы `stock` (available/reserved qty), `processed_events`
- Kafka consumer слушает `order.created`
- Пессимистичная блокировка: `SELECT ... WHERE sku = ANY($1) FOR UPDATE`
  — атомарное all-or-nothing резервирование нескольких SKU одной транзакцией
- Идемпотентность через таблицу `processed_events`
- Свой Outbox — публикует `stock.reserved` / `stock.failed`

### notification-service
Простой Kafka consumer, имитирует отправку уведомлений.
- Слушает `order.created`, `order.confirmed`, `order.cancelled`
- Идемпотентность через `inbox_events` с составным ключом `(order_id, event_type)`

## Паттерны и решения

| Паттерн | Где применяется |
|---|---|
| Outbox pattern | order-service, inventory-service — атомарная публикация событий |
| Saga (хореография через события) | order-service ↔ inventory-service |
| Inbox pattern | notification-service — идемпотентная обработка |
| Optimistic locking (`version`) | order-service — обновление статуса заказа |
| Pessimistic locking (`FOR UPDATE`) | inventory-service — резервирование товара |
| `FOR UPDATE SKIP LOCKED` | outbox worker'ы — конкурентная обработка без блокировок между воркерами |
| Cache-aside | api-gateway — кэш заказов в Redis |
| Token bucket rate limiting | api-gateway — Lua-скрипт, атомарный расчёт в Redis |
| Cursor-based pagination | order-service — список заказов |
| Circuit breaker | api-gateway → order-service *(план)* |
| Distributed tracing | trace_id через HTTP/gRPC/Kafka *(план)* |

## API

Все запросы — через API Gateway, `http://localhost:8080`.

| Метод | Путь | Описание |
|---|---|---|
| `POST` | `/api/v1/orders` | Создать заказ |
| `GET` | `/api/v1/orders/:id` | Получить заказ (cache-aside) |
| `GET` | `/api/v1/orders?customer_id=&cursor=` | Список заказов клиента |

Swagger UI: `http://localhost:8080/swagger/index.html`

## Стек

Go · Clean Architecture · PostgreSQL (pgx) · gin · gRPC (protobuf) ·
Kafka (segmentio/kafka-go) · Redis (go-redis) · Docker / Docker Compose ·
gomock · testify (table-driven, AAA)

## Запуск

```bash
docker compose up -d --build
```

Поднимет: 3× PostgreSQL (order/inventory/notification — database per service),
Kafka (KRaft, без Zookeeper), Redis, и все 7 Go-сервисов/воркеров.

Проверить статус:
```bash
docker compose ps
```

Пример запроса:
```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "11111111-1111-1111-1111-111111111111",
    "items": [
      {"sku": "SKU-001", "name": "Mechanical Keyboard", "price": 15000, "currency": "USD", "quantity": 1}
    ]
  }'
```

## Структура репозитория

```
mini-market/
├── api-gateway/          — HTTP Gateway, Redis, gRPC-клиент
├── order-service/         — ядро, Clean Architecture, Outbox, Saga-оркестрация
├── inventory-service/     — резервирование товара, pessimistic locking
├── notification-service/  — Kafka consumer, inbox pattern
└── docker-compose.yaml    — вся инфраструктура + сервисы одной командой
```

Каждый сервис — независимый Go-модуль со своим `go.mod`.

## Тесты

```bash
cd order-service
go test ./internal/usecase/... -v
```

Table-driven тесты, AAA (Arrange-Act-Assert), моки через `gomock`
(`go generate ./...` для перегенерации после изменения интерфейсов).

## Известные ограничения / roadmap

- Distributed tracing (trace_id через HTTP/gRPC/Kafka headers) — не реализовано
- Circuit breaker (Gateway → Order Service) — не реализовано
- CI (GitHub Actions) — не реализовано
- Proto-контракты сейчас копируются между order-service и api-gateway;
  планируется вынос в общий shared-модуль
- `domain.ErrOrderNotFound` — не заведена отдельная сентинел-ошибка,
  "not found" на gRPC-уровне маппится в `codes.Internal` вместо `codes.NotFound`