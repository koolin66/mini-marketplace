package logger

import (
	"log/slog"
	"os"
)

// New — создаёт JSON-логгер с указанием имени сервиса как постоянного поля.
// service попадёт в КАЖДУЮ строку лога автоматически — не нужно прописывать
// вручную в каждом вызове, что это именно inventory-service, а не order-service.
func New(serviceName string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})

	return slog.New(handler).With("service", serviceName)
}
