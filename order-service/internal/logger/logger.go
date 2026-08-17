package logger

import (
	"log/slog"
	"os"
)

// New — создаёт JSON-логгер с указанием имени сервиса как постоянного поля.
// service попадёт в КАЖДУЮ строку лога автоматически — не нужно прописывать
// вручную в каждом вызове, что это именно order-service, а не gateway.
func New(serviceName string) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, // порог: Debug-сообщения печататься не будут
	})

	return slog.New(handler).With("service", serviceName)
}
