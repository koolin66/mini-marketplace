package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal — Counter. Считает ВСЕ запросы, с лейблами для разбивки в Grafana.
	// Лейблы (method, path, status) превращают один счётчик в набор независимых временных
	// рядов — можно построить график "ошибки по конкретному эндпоинту" без доп. метрик.
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed by the gateway",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration — Histogram. Замеряет распределение латентности,
	// автоматически считает percentiles через prometheus_histogram_quantile в PromQL.
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets, // стандартные бакеты: 0.005, 0.01, 0.025, ... 10 секунд
		},
		[]string{"method", "path"},
	)
)
