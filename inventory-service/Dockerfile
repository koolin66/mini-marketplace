FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/inventory-service ./cmd/inventory-service
RUN go build -o /bin/inventory-outbox-worker ./cmd/inventory-outbox-worker

FROM alpine:latest AS inventory-service
WORKDIR /app
COPY --from=builder /bin/inventory-service .
COPY migrations ./migrations
CMD ["./inventory-service"]

FROM alpine:latest AS inventory-outbox-worker
WORKDIR /app
COPY --from=builder /bin/inventory-outbox-worker .
CMD ["./inventory-outbox-worker"]