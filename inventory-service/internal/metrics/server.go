package metrics

import (
	"log/slog"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// StartServer — запускает минимальный HTTP-сервер только с /metrics.
// Возвращает сам *http.Server, чтобы вызывающий код мог его грациозно остановить.
func StartServer(addr string, log *slog.Logger) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		log.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server failed", "error", err)
		}
	}()

	return srv
}
