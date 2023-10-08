package metrics

import (
	"fmt"
	"log"
	"strings"

	"github.com/boiler/ciri/config"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gaugeMap        = make(map[string]*prometheus.GaugeVec)
	countMap        = make(map[string]*prometheus.CounterVec)
	labelsFieldsMap = make(map[string][]string)
)

type PrometheusMetrics struct {
	GaugeNames []string
	CountNames []string
	Labels     []string
}

func InitPrometheus(cfg *config.Config, pms []*PrometheusMetrics) {

	for _, pm := range pms {
		for _, v := range pm.GaugeNames {
			labelsFieldsMap[v] = pm.Labels
			gaugeMap[v] = prometheus.NewGaugeVec(
				prometheus.GaugeOpts{
					Name: cfg.MetricsPrefix + "_" + v,
					Help: strings.Replace(v, "_", " ", -1),
				},
				labelsFieldsMap[v],
			)
		}
		for _, v := range pm.CountNames {
			labelsFieldsMap[v] = pm.Labels
			countMap[v] = prometheus.NewCounterVec(
				prometheus.CounterOpts{
					Name: cfg.MetricsPrefix + "_" + v,
					Help: strings.Replace(v, "_", " ", -1),
				},
				labelsFieldsMap[v],
			)
		}
	}

	for _, v := range gaugeMap {
		prometheus.MustRegister(v)
	}
	for _, v := range countMap {
		prometheus.MustRegister(v)
	}
}

func getLabels(m string, lv ...interface{}) prometheus.Labels {
	labels := make(prometheus.Labels)
	i := 0
	for i = 0; i < len(labelsFieldsMap[m]); i++ {
		f := labelsFieldsMap[m][i]
		switch t := lv[i].(type) {
		case string:
			labels[f] = fmt.Sprintf("%s", lv[i])
		case bool:
			labels[f] = fmt.Sprintf("%t", lv[i])
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			labels[f] = fmt.Sprintf("%d", lv[i])
		default:
			log.Printf("unknown metric label type: %T", t)
		}
	}
	return labels
}

func GaugeSet(k string, v float64, lv ...interface{}) {
	if p, ok := gaugeMap[k]; ok {
		p.With(getLabels(k, lv...)).Set(v)
	}
}

func GaugeInc(k string, lv ...interface{}) {
	if p, ok := gaugeMap[k]; ok {
		p.With(getLabels(k, lv...)).Inc()
	}
}

func GaugeDec(k string, lv ...interface{}) {
	if p, ok := gaugeMap[k]; ok {
		p.With(getLabels(k, lv...)).Dec()
	}
}

func CountAdd(k string, v float64, lv ...interface{}) {
	if p, ok := countMap[k]; ok {
		p.With(getLabels(k, lv...)).Add(v)
	}
}
