package opentsdbclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"net"
)

const DefaultAPIURL = "http://locahost/api"

type Client struct {
	poster                Poster
	metrics               []Metric
	prefix                string
	deployment            string
	ip                    string
	totalMessagesReceived float64
	totalMetricsSent      float64
	hasSlowAlert          bool
}

type Tags struct {
	Deployment string `json:"deployment"`
	Job        string `json:"job"`
	Index      int    `json:"index"`
	IP         string `json:"ip"`
}

type Metric struct {
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Tags      Tags    `json:"tags"`
}

func New(poster Poster, prefix string, deployment string, ip string) *Client {
	return &Client{
		poster:     poster,
		prefix:     prefix,
		deployment: deployment,
		ip:         ip,
	}
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	c.totalMessagesReceived++
	if envelope.GetEventType() != events.Envelope_ValueMetric && envelope.GetEventType() != events.Envelope_CounterEvent {
		return
	}
	metric := Metric{
		Value:     getValue(envelope),
		Timestamp: envelope.GetTimestamp() / int64(time.Second),
		Metric:    c.prefix + getName(envelope),
		Tags:      getTags(envelope),
	}

	c.metrics = append(c.metrics, metric)
}

func (c *Client) AlertSlowConsumerError() {
	if !c.hasSlowAlert {
		c.hasSlowAlert = true
		c.addInternalMetric("slowConsumerAlert", 1)
	}
}

func (c *Client) addInternalMetric(name string, value float64) {
	internalMetric := Metric{
		Metric:    c.prefix + name,
		Value:     value,
		Timestamp: time.Now().Unix(),
		Tags: Tags{
			Deployment: c.deployment,
			IP:         c.ip,
		},
	}

	c.metrics = append(c.metrics, internalMetric)
}

func (c *Client) PostMetrics() error {
	c.populateInternalMetrics()
	numMetrics := len(c.metrics)
	err := c.poster.Post(c.metrics)
	if err != nil {
		return err
	}

	c.totalMetricsSent += float64(numMetrics)
	c.hasSlowAlert = false

	c.metrics = nil
	return nil
}

func (c *Client) populateInternalMetrics() {
	c.addInternalMetric("totalMessagesReceived", c.totalMessagesReceived)
	c.addInternalMetric("totalMetricsSent", c.totalMetricsSent)

	if !c.hasSlowAlert {
		c.addInternalMetric("slowConsumerAlert", 0)
	}
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

func getTags(envelope *events.Envelope) Tags {
	log.Printf("Tags: %s\n", envelope.GetIndex())
	index, err := strconv.Atoi(envelope.GetIndex())
	if err != nil {
		log.Printf("Invalid Index \"%s\" provided, using default index 0\n", envelope.GetIndex())
		index = 0
	}
	log.Printf("deployment %s, index %d\n", envelope.GetDeployment(), index)
	ret := Tags{envelope.GetDeployment(), envelope.GetJob(), index, envelope.GetIp()}
	log.Println(ret)
	return ret
}

type Poster interface {
	Post([]Metric) error
}

type HTTPPoster struct {
	tsdbHost string
}

func NewHTTPPoster(tsdbHost string) *HTTPPoster {
	return &HTTPPoster{
		tsdbHost: tsdbHost,
	}
}

func (p *HTTPPoster) Post(metrics []Metric) error {
	numMetrics := len(metrics)
	log.Printf("Posting %d metrics", numMetrics)
	url := p.tsdbURL()
	seriesBytes := p.formatMetrics(metrics)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(seriesBytes))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode >= 300 || resp.StatusCode < 200 {
		contents, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("%s", err)
		}
		log.Printf("Response body is: %s", string(contents))
		return fmt.Errorf("opentsdb request returned HTTP response: %v", resp.StatusCode)
	}
	return nil
}

func (p *HTTPPoster) tsdbURL() string {
	url := fmt.Sprintf("%s/put?details", p.tsdbHost)
	return url
}

func (p *HTTPPoster) formatMetrics(metrics []Metric) []byte {
	encodedMetric, _ := json.Marshal(metrics)
	return encodedMetric
}

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
		metricString += fmt.Sprintf(" index=%d", metric.Tags.Index)
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
