package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// MessagesProcessedTotal — Counter с лейблом result: "reserved" / "insufficient_stock" /
	// "already_processed" / "error". Разбивка по исходу обработки, не просто общий счётчик.
	MessagesProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inventory_messages_processed_total",
			Help: "Total number of order.created messages processed",
		},
		[]string{"result"},
	)

	MessageProcessingDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "inventory_message_processing_duration_seconds",
			Help:    "Time spent processing a single order.created message",
			Buckets: prometheus.DefBuckets,
		},
	)

	OutboxPendingRecords = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "inventory_outbox_pending_records",
			Help: "Current number of PENDING records in the outbox table",
		},
	)
)
