package metrics

import (
	"github.com/boiler/ciri/config"
)

func Init(cfg *config.Config) {
	prometheusMetrics := []*PrometheusMetrics{
		&PrometheusMetrics{
			GaugeNames: []string{"tasks_count"},
			Labels:     []string{"sticker", "priority", "pool", "state"},
		},
		&PrometheusMetrics{
			CountNames: []string{"tasks_acquired", "tasks_refused", "tasks_inserted", "tasks_update", "tasks_deleted"},
			Labels:     []string{"sticker", "priority", "pool"},
		},
		&PrometheusMetrics{
			CountNames: []string{"tasks_done"},
			Labels:     []string{"sticker", "priority", "pool", "error"},
		},
	}
	InitPrometheus(cfg, prometheusMetrics)
}
