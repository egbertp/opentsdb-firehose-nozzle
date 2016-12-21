package opentsdbclient

import (
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/poster"
)

const DefaultAPIURL = "http://locahost/api"

type Poster interface {
	Post([]poster.Metric) error
}

type Client struct {
	transporter              Poster
	metrics                  []poster.Metric
	prefix                   string
	deployment               string
	job                      string
	index                    string
	ip                       string
	totalMessagesReceived    float64
	totalMetricsSent         float64
	totalFirehoseDisconnects float64
}

func New(transporter Poster, prefix string, deployment string, job string, index string, ip string) *Client {
	return &Client{
		transporter: transporter,
		prefix:      prefix,
		deployment:  deployment,
		job:         job,
		index:       index,
		ip:          ip,
	}
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	c.totalMessagesReceived++
	if envelope.GetEventType() != events.Envelope_ValueMetric && envelope.GetEventType() != events.Envelope_CounterEvent {
		return
	}
	metric := poster.Metric{
		Value:     getValue(envelope),
		Timestamp: envelope.GetTimestamp() / int64(time.Second),
		Metric:    c.prefix + getName(envelope),
		Tags:      getTags(envelope),
	}

	c.metrics = append(c.metrics, metric)
}

func (c *Client) addInternalMetric(name string, value float64) {
	internalMetric := poster.Metric{
		Metric:    c.prefix + name,
		Value:     value,
		Timestamp: time.Now().Unix(),
		Tags: poster.Tags{
			Deployment: c.deployment,
			IP:         c.ip,
			Job:        c.job,
			Index:      c.index,
		},
	}

	c.metrics = append(c.metrics, internalMetric)
}

func (c *Client) PostMetrics() error {
	c.populateInternalMetrics()
	numMetrics := len(c.metrics)
	err := c.transporter.Post(c.metrics)
	if err != nil {
		return err
	}

	c.totalMetricsSent += float64(numMetrics)
	c.metrics = nil
	return nil
}

func (c *Client) populateInternalMetrics() {
	c.addInternalMetric("totalMessagesReceived", c.totalMessagesReceived)
	c.addInternalMetric("totalMetricsSent", c.totalMetricsSent)
	c.addInternalMetric("totalFirehoseDisconnects", c.totalFirehoseDisconnects)
}

func getName(envelope *events.Envelope) string {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetOrigin() + "." + envelope.GetValueMetric().GetName()
	case events.Envelope_CounterEvent:
		return envelope.GetOrigin() + "." + envelope.GetCounterEvent().GetName()
	default:
		return ""
	}
}

func getValue(envelope *events.Envelope) float64 {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetValueMetric().GetValue()
	case events.Envelope_CounterEvent:
		return float64(envelope.GetCounterEvent().GetTotal())
	default:
		return 0
	}
}

func getTags(envelope *events.Envelope) poster.Tags {
	ret := poster.Tags{
		Deployment: envelope.GetDeployment(),
		Job:        envelope.GetJob(),
		Index:      envelope.GetIndex(),
		IP:         envelope.GetIp(),
	}
	return ret
}

func (c *Client) IncrementFirehoseDisconnect() {
	c.totalFirehoseDisconnects++
}