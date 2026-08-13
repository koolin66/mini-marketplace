FROM golang:1.25-alpine AS builder
WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o /bin/gateway ./cmd/gateway

FROM alpine:latest
WORKDIR /app
COPY --from=builder /bin/gateway .
CMD ["./gateway"]