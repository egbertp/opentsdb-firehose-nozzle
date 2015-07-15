package opentsdbclient_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"../opentsdbclient"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var bodyChan chan []byte

var _ = Describe("OpentsdbClient", func() {

	var ts *httptest.Server

	BeforeEach(func() {
		bodyChan = make(chan []byte, 1)
		ts = httptest.NewServer(http.HandlerFunc(handlePost))
	})

	It("posts ValueMetrics in JSON format", func() {
		c := opentsdbclient.New(ts.URL, "")

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(76),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`[
          {
            "metric": "origin.metricName",
            "value": 5,
            "timestamp": 1,
            "tags": [
              "deployment:deployment-name",
              "job:doppler"
            ]
          },
          {
            "metric": "origin.metricName",
            "value": 76,
            "timestamp": 2,
            "tags": [
              "deployment:deployment-name",
              "job:doppler"
            ]
          }
        ]`)))
	})

	It("registers metrics with the same name but different tags as different", func() {
		c := opentsdbclient.New(ts.URL, "")

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(76),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("gorouter"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))

		Expect(receivedBytes).To(ContainSubstring(`["deployment:deployment-name","job:doppler"]`))
		Expect(receivedBytes).To(ContainSubstring(`["deployment:deployment-name","job:gorouter"]`))
	})

	It("posts CounterEvents in JSON format and empties map after post", func() {
		c := opentsdbclient.New(ts.URL, "")

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(1),
				Total: proto.Uint64(5),
			},
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(6),
				Total: proto.Uint64(11),
			},
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`[
			{
        "metric": "origin.counterName",
        "value": 5,
        "timestamp": 1
      },
      {
        "metric": "origin.counterName",
        "value": 11,
        "timestamp": 2
      }
		]`)))

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(MatchJSON(`[]`)))
	})

})

func handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic("No body!")
	}

	bodyChan <- body
}
