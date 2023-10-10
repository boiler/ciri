package metrics

import (
	"fmt"
	"log"

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
	for k, v := range cfg.WorkerCustomMetric {
		if v.Type == "gauge" {
			prometheusMetrics = append(prometheusMetrics, &PrometheusMetrics{
				GaugeNames: []string{k},
				Labels:     v.Labels,
			})
		} else if v.Type == "count" {
			prometheusMetrics = append(prometheusMetrics, &PrometheusMetrics{
				CountNames: []string{k},
				Labels:     v.Labels,
			})
		} else {
			log.Panic("unknown metric type in config, only 'gauge' and 'count' supported")
		}
	}
	InitPrometheus(cfg, prometheusMetrics)
}

type PostMetric struct {
	Add    float64       `json:"add"`
	Set    float64       `json:"set"`
	Inc    bool          `json:"inc"`
	Dec    bool          `json:"dec"`
	Labels []interface{} `json:"labels"`
}

func ApplyPostMetrics(cfg *config.Config, pm map[string]*PostMetric) error {
	for k, m := range pm {
		if cfgm, ok := cfg.WorkerCustomMetric[k]; ok {
			if len(m.Labels) != len(cfgm.Labels) {
				return fmt.Errorf("metrics labels count mismatch with config")
			}
			if m.Add != 0 {
				CountAdd(k, m.Add, m.Labels...)
			}
			if m.Set != 0 {
				GaugeSet(k, m.Set, m.Labels...)
			}
			if m.Inc {
				GaugeInc(k, m.Labels...)
			}
			if m.Dec {
				GaugeDec(k, m.Labels...)
			}
		}
	}
	return nil
}
