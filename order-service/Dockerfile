# Stage 1: сборка — общий для обоих бинарников
FROM golang:1.25-alpine AS builder
WORKDIR /app

# Сначала копируем только go.mod/go.sum — слой кэшируется отдельно от исходников,
# пересобирается только если реально поменялись зависимости, а не при каждом изменении кода.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/order-service ./cmd/order-service
RUN go build -o /bin/outbox-worker ./cmd/outbox-worker

# Stage 2a: финальный образ для сервера
FROM alpine:latest AS order-service
WORKDIR /app
COPY --from=builder /bin/order-service .
COPY migrations ./migrations
CMD ["./order-service"]

# Stage 2b: финальный образ для воркера
FROM alpine:latest AS outbox-worker
WORKDIR /app
COPY --from=builder /bin/outbox-worker .
CMD ["./outbox-worker"]