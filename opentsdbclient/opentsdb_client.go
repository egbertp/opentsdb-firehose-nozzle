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
)

const DefaultAPIURL = "http://locahost/api"

type Client struct {
	apiURL                string
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

func New(apiURL string, prefix string, deployment string, ip string) *Client {
	return &Client{
		apiURL:     apiURL,
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
	log.Printf("Posting %d metrics", numMetrics)
	url := c.tsdbURL()
	seriesBytes := c.formatMetrics()
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

	c.totalMetricsSent += float64(numMetrics)
	c.hasSlowAlert = false

	c.metrics = nil
	return nil
}

func (c *Client) tsdbURL() string {
	url := fmt.Sprintf("%s/put?details", c.apiURL)
	return url
}

func (c *Client) formatMetrics() []byte {
	encodedMetric, _ := json.Marshal(c.metrics)
	return encodedMetric
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
