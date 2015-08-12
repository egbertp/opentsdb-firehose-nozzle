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
	apiURL       string
	metricPoints map[metricKey]metricValue
	prefix       string
}

func New(apiURL string, prefix string) *Client {
	return &Client{
		apiURL:       apiURL,
		metricPoints: make(map[metricKey]metricValue),
		prefix:       prefix,
	}
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	key := metricKey{
		eventType:  envelope.GetEventType(),
		name:       getName(envelope),
		deployment: envelope.GetDeployment(),
		job:        envelope.GetJob(),
		index:      envelope.GetIndex(),
		ip:         envelope.GetIp(),
	}

	mVal := c.metricPoints[key]
	value := getValue(envelope)

	mVal.tags = getTags(envelope)
	mVal.points = append(mVal.points, point{
		timestamp: envelope.GetTimestamp() / int64(time.Second),
		value:     value,
	})

	c.metricPoints[key] = mVal
}

func (c *Client) PostAllMetrics() error {
	var err error
	numMetrics := len(c.metricPoints)
	if numMetrics > 50 {
		i := 0
		someMetrics := make(map[metricKey]metricValue)
		for k, v := range c.metricPoints {
			someMetrics[k] = v
			i := i + 1
			if i >= 50 {
				err = c.postMetrics(someMetrics)

				if err != nil {
					log.Printf("PostAllMetrics Error %s", err.Error())
				}

				i = 0
				someMetrics = make(map[metricKey]metricValue)
			}
		}

		if i > 0 {
			err = c.postMetrics(someMetrics)
		}
	} else {
		err = c.postMetrics(c.metricPoints)
	}

	return err
}

func (c *Client) postMetrics(metrics map[metricKey]metricValue) error {
	numMetrics := len(metrics)
	log.Printf("Posting %d metrics", numMetrics)
	url := c.seriesURL()
	seriesBytes := c.formatMetrics(metrics)
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
		return fmt.Errorf("opentsdb request returned HTTP status code: %v", resp.StatusCode)
	}

	c.metricPoints = make(map[metricKey]metricValue)
	return nil
}

func (c *Client) seriesURL() string {
	url := fmt.Sprintf("%s/put?details", c.apiURL)
	return url
}

func (c *Client) formatMetrics(inMetrics map[metricKey]metricValue) []byte {
	metrics := []metric{}
	for key, mVal := range inMetrics {
		for _, p := range mVal.points {
			metrics = append(metrics, metric{
				Metric:    c.prefix + key.name,
				Timestamp: p.timestamp,
				Value:     p.value,
				Tags:      mVal.tags,
			})
		}
	}

	encodedMetric, _ := json.Marshal(metrics)

	return encodedMetric
}

type metricKey struct {
	eventType  events.Envelope_EventType
	name       string
	deployment string
	job        string
	index      string
	ip         string
}

type metricValue struct {
	tags   tags
	points []point
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

func getTags(envelope *events.Envelope) tags {
	log.Printf("Tags: %s\n", envelope.GetIndex())
	index, err := strconv.Atoi(envelope.GetIndex())
	if err != nil {
		log.Printf("Invalid Index \"%s\" provided, using default index 0\n", envelope.GetIndex())
		index = 0
	}
	log.Printf("deployment %s, index %d\n", envelope.GetDeployment(), index)
	ret := tags{envelope.GetDeployment(), envelope.GetJob(), index, envelope.GetIp()}
	log.Println(ret)
	return ret
}

func appendTagIfNotEmpty(tags []string, key string, value string) []string {
	if value != "" {
		tags = append(tags, fmt.Sprintf("%s:%s", key, value))
	}
	return tags
}

type point struct {
	timestamp int64
	value     float64
}

func (p point) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`[%d, %f]`, p.timestamp, p.value)), nil
}

type metric struct {
	Metric    string  `json:"metric"`
	Value     float64 `json:"value"`
	Timestamp int64   `json:"timestamp"`
	Host      string  `json:"host,omitempty"`
	Tags      tags    `json:"tags"`
}

type tags struct {
	Deployment string `json:"deployment"`
	Job        string `json:"job"`
	Index      int    `json:"index"`
	Ip         string `json:"ip"`
}

type payload struct {
	Series []metric `json:"series"`
}
