package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

type Metrics struct {
	ConsumeTotal    *prometheus.CounterVec
	ConsumeDuration *prometheus.HistogramVec
	InboxDuplicate  prometheus.Counter
	SendTotal       *prometheus.CounterVec
}

func New(reg prometheus.Registerer) *Metrics {
	m := &Metrics{
		ConsumeTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notification",
				Name:      "consume_total",
				Help:      "Total number of consumed trip events",
			},
			[]string{"result"},
		),
		ConsumeDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "notification",
				Name:      "consume_duration_seconds",
				Help:      "Duration of trip event consumption in seconds",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"result"},
		),
		InboxDuplicate: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "notification",
				Name:      "inbox_duplicate_total",
				Help:      "Total number of duplicate events skipped by inbox",
			},
		),
		SendTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "notification",
				Name:      "send_total",
				Help:      "Total number of notification send attempts",
			},
			[]string{"result"},
		),
	}

	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.ConsumeTotal,
		m.ConsumeDuration,
		m.InboxDuplicate,
		m.SendTotal,
	)

	return m
}
