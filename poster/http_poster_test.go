package poster_test

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/poster"

	"encoding/json"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"fmt"
	"log"
	"strings"
)

var bodyChan chan []byte
var responseCode int

var _ = Describe("OpentsdbClient", func() {

	var (
		ts *httptest.Server
		p  *poster.HTTPPoster
	)

	BeforeEach(func() {
		bodyChan = make(chan []byte, 1)
		responseCode = http.StatusOK
		ts = httptest.NewServer(http.HandlerFunc(handlePost))
		p = poster.NewHTTPPoster(ts.URL)
	})

	It("posts metrics in JSON format", func() {
		metric1 := poster.Metric{
			Metric:    "origin.metricName",
			Value:     5,
			Timestamp: 1,
			Tags: poster.Tags{
				Deployment: "deployment-name",
				Job:        "doppler",
				Index:      "SOME-GUID",
				IP:         "10.10.10.10",
			},
		}
		metric2 := poster.Metric{
			Metric:    "origin.metricName",
			Value:     76,
			Timestamp: 2,
			Tags: poster.Tags{
				Deployment: "deployment-name",
				Job:        "doppler",
				Index:      "SOME-GUID-2",
			},
		}

		err := p.Post([]poster.Metric{metric1, metric2})
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))

		var metrics []poster.Metric
		buf := bytes.NewBuffer(receivedBytes)
		gzipReader, _ := gzip.NewReader(buf)
		uncompressedData, _ := ioutil.ReadAll(gzipReader)
		err = json.Unmarshal(uncompressedData, &metrics)
		Expect(err).NotTo(HaveOccurred())

		Expect(metrics).To(ContainElement(
			poster.Metric{
				Metric:    "origin.metricName",
				Value:     5,
				Timestamp: 1,
				Tags: poster.Tags{
					Deployment: "deployment-name",
					Job:        "doppler",
					Index:      "SOME-GUID",
					IP:         "10.10.10.10",
				},
			}))

		Expect(metrics).To(ContainElement(
			poster.Metric{
				Metric:    "origin.metricName",
				Value:     76,
				Timestamp: 2,
				Tags: poster.Tags{
					Deployment: "deployment-name",
					Job:        "doppler",
					Index:      "SOME-GUID-2",
					IP:         "",
				},
			}))
	})

	It("returns an error when opentsdb responds with a non 200 response code", func() {
		responseCode = http.StatusBadRequest // 400
		err := p.Post([]poster.Metric{})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("opentsdb request returned HTTP response: 400"))
		<-bodyChan

		responseCode = http.StatusSwitchingProtocols // 101
		err = p.Post([]poster.Metric{})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("opentsdb request returned HTTP response: 101"))
		<-bodyChan

		responseCode = http.StatusAccepted // 201
		err = p.Post([]poster.Metric{})
		Expect(err).ToNot(HaveOccurred())
	})

	It("shows a proper error message when the server does not respond", func() {
		address := ts.Listener.Addr().String()
		ts.Close()
		err := p.Post([]poster.Metric{})

		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError(fmt.Sprintf("Post http://%s/put?details: dial tcp %s: getsockopt: connection refused", address, address)))
	})
})

func handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.URL.Path == "/put" && strings.Contains(r.URL.RawQuery, "details") {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic("No body!")
		}

		bodyChan <- body
		w.WriteHeader(responseCode)
	} else {
		log.Println("Unexpected Path. Opentsdb HTTP API is listening on /put?details")
		w.WriteHeader(http.StatusInternalServerError)
	}
}
