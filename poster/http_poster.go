package poster

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

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
