package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// NotificationsProcessedTotal — Counter с лейблами event_type (order.created/confirmed/cancelled)
	// и result (sent/already_processed/error). Двумерная разбивка — можно построить график
	// "сколько уведомлений об отмене отправлено" отдельно от "сколько было дублей".
	NotificationsProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "notifications_processed_total",
			Help: "Total number of order events processed by notification-service",
		},
		[]string{"event_type", "result"},
	)

	NotificationProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "notification_processing_duration_seconds",
			Help:    "Time spent processing a single order event",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"event_type"},
	)
)
