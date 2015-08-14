package opentsdbclient_test

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"

	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/opentsdbclient"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/poster"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"

	"encoding/json"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"time"
)

var bodyChan chan []byte
var responseCode int

var _ = Describe("OpentsdbClient", func() {

	var (
		ts *httptest.Server
		c  *opentsdbclient.Client
		p  opentsdbclient.Poster
	)

	BeforeEach(func() {
		bodyChan = make(chan []byte, 1)
		responseCode = http.StatusOK
		ts = httptest.NewServer(http.HandlerFunc(handlePost))
		p = poster.NewHTTPPoster(ts.URL)
		c = opentsdbclient.New(p, "opentsdb.nozzle.", "test-deployment", "dummy-ip")
	})

	It("ignores messages that aren't value metrics or counter events", func() {
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_LogMessage.Enum(),
			LogMessage: &events.LogMessage{
				Message:     []byte("log message"),
				MessageType: events.LogMessage_OUT.Enum(),
				Timestamp:   proto.Int64(1000000000),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ContainerMetric.Enum(),
			ContainerMetric: &events.ContainerMetric{
				ApplicationId: proto.String("app-id"),
				InstanceIndex: proto.Int32(4),
				CpuPercentage: proto.Float64(20.0),
				MemoryBytes:   proto.Uint64(19939949),
				DiskBytes:     proto.Uint64(29488929),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))

		var metrics []poster.Metric
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(metrics).To(HaveLen(3))

		validateMetrics(metrics, 2, 0)

	})

	It("posts ValueMetrics in JSON format", func() {
		c = opentsdbclient.New(p, "", "test-deployment", "dummy-ip")

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
			Index:      proto.String("0"),
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
			Index:      proto.String("0"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))

		var metrics []poster.Metric
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())

		Expect(metrics).To(ContainElement(
			poster.Metric{
				Metric:    "origin.metricName",
				Value:     5,
				Timestamp: 1,
				Tags: poster.Tags{
					Deployment: "deployment-name",
					Job:        "doppler",
					Index:      0,
					IP:         "",
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
					Index:      0,
					IP:         "",
				},
			}))
	})

	It("posts CounterEvent in JSON format", func() {
		c = opentsdbclient.New(p, "", "test-deployment", "dummy-ip")

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("metricName"),
				Total: proto.Uint64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("0"),
		})

		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("metricName"),
				Total: proto.Uint64(76),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("0"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))

		var metrics []poster.Metric
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())

		Expect(metrics).To(ContainElement(
			poster.Metric{
				Metric:    "origin.metricName",
				Value:     5,
				Timestamp: 1,
				Tags: poster.Tags{
					Deployment: "deployment-name",
					Job:        "doppler",
					Index:      0,
					IP:         "",
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
					Index:      0,
					IP:         "",
				},
			}))
	})

	It("registers metrics with the same name but different tags as different", func() {
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

		Expect(receivedBytes).To(ContainSubstring(`"deployment":"deployment-name","job":"doppler"`))
		Expect(receivedBytes).To(ContainSubstring(`"deployment":"deployment-name","job":"gorouter"`))
	})

	It("posts CounterEvents in JSON format and empties map after post", func() {
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
		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))
		var metrics []poster.Metric
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())
		validateMetrics(metrics, 2, 0)

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
		Eventually(bodyChan).Should(Receive(&receivedBytes))
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())
		validateMetrics(metrics, 2, 5)
	})

	It("sends a value 1 for the slowConsumerAlert metric when consumer error is set", func() {
		c.AlertSlowConsumerError()

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))
		var metrics []poster.Metric
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(metrics).To(HaveLen(3))

		errMetric := findSlowConsumerMetric(metrics)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Value).To(BeEquivalentTo(1))

	})

	It("sends a value 0 for the slowConsumerAlert metric when consumer error is not set", func() {
		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))
		var metrics []poster.Metric
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(metrics).To(HaveLen(3))

		errMetric := findSlowConsumerMetric(metrics)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Value).To(BeEquivalentTo(0))
	})

	It("unsets the slow consumer error once it publishes the alert to opentsdb", func() {
		c.AlertSlowConsumerError()

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(bodyChan).Should(Receive(&receivedBytes))
		var metrics []poster.Metric
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(metrics).To(HaveLen(3))

		errMetric := findSlowConsumerMetric(metrics)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Value).To(BeEquivalentTo(1))

		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		Eventually(bodyChan).Should(Receive(&receivedBytes))
		err = json.Unmarshal(receivedBytes, &metrics)
		Expect(err).NotTo(HaveOccurred())
		Expect(metrics).To(HaveLen(3))

		errMetric = findSlowConsumerMetric(metrics)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Value).To(BeEquivalentTo(0))
	})

	It("returns an error when opentsdb responds with a non 200 response code", func() {
		responseCode = http.StatusBadRequest // 400
		err := c.PostMetrics()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("opentsdb request returned HTTP response: 400"))
		<-bodyChan

		responseCode = http.StatusSwitchingProtocols // 101
		err = c.PostMetrics()
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("opentsdb request returned HTTP response: 101"))
		<-bodyChan

		responseCode = http.StatusAccepted // 201
		err = c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())
	})

})

func validateMetrics(metrics []poster.Metric, totalMessagesReceived int, totalMetricsSent int) {
	totalMessagesReceivedFound := false
	totalMetricsSentFound := false
	for _, metric := range metrics {
		internalMetric := false
		var metricValue int
		if metric.Metric == "opentsdb.nozzle.totalMessagesReceived" {
			totalMessagesReceivedFound = true
			internalMetric = true
			metricValue = totalMessagesReceived
		}
		if metric.Metric == "opentsdb.nozzle.totalMetricsSent" {
			totalMetricsSentFound = true
			internalMetric = true
			metricValue = totalMetricsSent
		}

		if internalMetric {
			Expect(metric.Timestamp).To(BeNumerically(">", time.Now().Unix()-10), "Timestamp should not be less than 10 seconds ago")
			Expect(metric.Value).To(Equal(float64(metricValue)))
			Expect(metric.Tags).To(Equal(poster.Tags{
				Deployment: "test-deployment",
				IP:         "dummy-ip",
			}))
		}
	}
	Expect(totalMessagesReceivedFound).To(BeTrue())
	Expect(totalMetricsSentFound).To(BeTrue())
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic("No body!")
	}

	bodyChan <- body
	w.WriteHeader(responseCode)
}

func findSlowConsumerMetric(metrics []poster.Metric) *poster.Metric {
	for _, metric := range metrics {
		if metric.Metric == "opentsdb.nozzle.slowConsumerAlert" {
			return &metric
		}
	}
	return nil
}
