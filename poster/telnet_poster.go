package poster

import (
	"fmt"
	"net"
)

type TelnetPoster struct {
	tsdbHost string
}

func NewTelnetPoster(tsdbHost string) *TelnetPoster {
	return &TelnetPoster{
		tsdbHost: tsdbHost,
	}
}

func (p *TelnetPoster) Post(metrics []Metric) error {
	conn, err := net.Dial("tcp", p.tsdbHost)
	if err != nil {
		return err
	}

	defer conn.Close()

	_, err = conn.Write([]byte(p.formatMetrics(metrics)))
	return err
}

func (p *TelnetPoster) formatMetrics(metrics []Metric) []byte {
	var result []byte
	for _, metric := range metrics {
		metricString := fmt.Sprintf("put %s %d %f",
			metric.Metric,
			metric.Timestamp,
			metric.Value,
		)
		if metric.Tags.Deployment != "" {
			metricString += fmt.Sprintf(" deployment=%s", metric.Tags.Deployment)
		}
		metricString += fmt.Sprintf(" index=%s", metric.Tags.Index)
		if metric.Tags.IP != "" {
			metricString += fmt.Sprintf(" ip=%s", metric.Tags.IP)
		}
		if metric.Tags.Job != "" {
			metricString += fmt.Sprintf(" job=%s", metric.Tags.Job)
		}
		metricString += "\n"
		result = append(result, []byte(metricString)...)
	}
	return result
}
