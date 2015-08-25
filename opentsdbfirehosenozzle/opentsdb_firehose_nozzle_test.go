package opentsdbfirehosenozzle_test

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/nozzleconfig"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/opentsdbfirehosenozzle"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/poster"
	. "github.com/pivotal-cloudops/opentsdb-firehose-nozzle/testhelpers"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/uaatokenfetcher"
)

var _ = Describe("OpenTSDB Firehose Nozzle", func() {
	var fakeUAA *FakeUAA
	var fakeFirehose *FakeFirehose
	var fakeOpenTSDB *FakeOpenTSDB
	var config *nozzleconfig.NozzleConfig
	var nozzle *opentsdbfirehosenozzle.OpenTSDBFirehoseNozzle
	var logOutput *gbytes.Buffer
	var tokenFetcher *uaatokenfetcher.UAATokenFetcher

	BeforeEach(func() {
		fakeUAA = NewFakeUAA("bearer", "123456789")
		fakeToken := fakeUAA.AuthToken()
		fakeFirehose = NewFakeFirehose(fakeToken)
		fakeOpenTSDB = NewFakeOpenTSDB()

		fakeUAA.Start()
		fakeFirehose.Start()
		fakeOpenTSDB.Start()

		tokenFetcher = &uaatokenfetcher.UAATokenFetcher{
			UaaUrl: fakeUAA.URL(),
		}

		config = &nozzleconfig.NozzleConfig{
			UAAURL:               fakeUAA.URL(),
			OpenTSDBURL:          fakeOpenTSDB.URL(),
			FlushDurationSeconds: 10,
			TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
			DisableAccessControl: false,
			MetricPrefix:         "opentsdb.nozzle.",
			MaxBufferSize:        50,
		}

		logOutput = gbytes.NewBuffer()
		log.SetOutput(logOutput)
		nozzle = opentsdbfirehosenozzle.NewOpenTSDBFirehoseNozzle(config, tokenFetcher)
	})

	AfterEach(func() {
		fakeUAA.Close()
		fakeFirehose.Close()
		fakeOpenTSDB.Close()
	})

	It("sends metrics when the FlushDurationTicker ticks", func(done Done) {
		defer close(done)
		fakeFirehose.KeepConnectionAlive()
		defer fakeFirehose.CloseAliveConnection()

		config.FlushDurationSeconds = 1
		nozzle = opentsdbfirehosenozzle.NewOpenTSDBFirehoseNozzle(config, tokenFetcher)

		envelope := events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(float64(1)),
				Unit:  proto.String("gauge"),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		}
		fakeFirehose.AddEvent(envelope)

		go nozzle.Start()

		var contents []byte
		Eventually(fakeOpenTSDB.ReceivedContents, 2).Should(Receive(&contents))

		var metrics []poster.Metric
		err := json.Unmarshal(contents, &metrics)
		Expect(err).ToNot(HaveOccurred())

		Expect(logOutput).ToNot(gbytes.Say("Error while reading from the firehose"))

		// +3 internal metrics that show totalMessagesReceived, totalMetricSent, and slowConsumerAlert
		Expect(metrics).To(HaveLen(4))
	}, 3)

	It("sends a server disconnected metric when the server disconnects abnormally", func(done Done) {
		defer close(done)

		for i := 0; i < 10; i++ {
			envelope := events.Envelope{
				Origin:    proto.String("origin"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_ValueMetric.Enum(),
				ValueMetric: &events.ValueMetric{
					Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
					Value: proto.Float64(float64(i)),
					Unit:  proto.String("gauge"),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
				Index:      proto.String("123"),
			}
			fakeFirehose.AddEvent(envelope)
		}

		fakeFirehose.SetCloseMessage(websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Client did not respond to ping before keep-alive timeout expired."))

		go nozzle.Start()

		var contents []byte
		Eventually(fakeOpenTSDB.ReceivedContents).Should(Receive(&contents))

		var metrics []poster.Metric
		err := json.Unmarshal(contents, &metrics)
		Expect(err).ToNot(HaveOccurred())

		slowConsumerMetric := findSlowConsumerMetric(metrics)
		Expect(slowConsumerMetric).NotTo(BeNil())
		Expect(slowConsumerMetric.Value).To(BeEquivalentTo(1))

		Expect(logOutput).To(gbytes.Say("Error while reading from the firehose"))
		Expect(logOutput).To(gbytes.Say("Client did not respond to ping before keep-alive timeout expired."))
		Expect(logOutput).To(gbytes.Say("Disconnected because nozzle couldn't keep up."))
	}, 2)

	It("sends metrics when there are more messages than buffersize", func(done Done) {
		defer close(done)
		fakeFirehose.KeepConnectionAlive()
		defer fakeFirehose.CloseAliveConnection()

		config.MaxBufferSize = 30
		nozzle = opentsdbfirehosenozzle.NewOpenTSDBFirehoseNozzle(config, tokenFetcher)

		for i := 0; i < 31; i++ {
			envelope := events.Envelope{
				Origin:    proto.String("origin"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_ValueMetric.Enum(),
				ValueMetric: &events.ValueMetric{
					Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
					Value: proto.Float64(float64(i)),
					Unit:  proto.String("gauge"),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
			}
			fakeFirehose.AddEvent(envelope)
		}

		go nozzle.Start()

		var contents []byte
		Eventually(fakeOpenTSDB.ReceivedContents).Should(Receive(&contents))

		var metrics []poster.Metric
		err := json.Unmarshal(contents, &metrics)
		Expect(err).ToNot(HaveOccurred())

		Expect(logOutput).ToNot(gbytes.Say("Error while reading from the firehose"))

		// +3 internal metrics that show totalMessagesReceived, totalMetricSent, and slowConsumerAlert
		Expect(metrics).To(HaveLen(34))
	})

	It("receives data from the firehose", func(done Done) {
		defer close(done)

		for i := 0; i < 10; i++ {
			envelope := events.Envelope{
				Origin:    proto.String("origin"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_ValueMetric.Enum(),
				ValueMetric: &events.ValueMetric{
					Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
					Value: proto.Float64(float64(i)),
					Unit:  proto.String("gauge"),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
				Index:      proto.String("0"),
			}
			fakeFirehose.AddEvent(envelope)
		}

		go nozzle.Start()

		var contents []byte
		Eventually(fakeOpenTSDB.ReceivedContents).Should(Receive(&contents))

		var metrics []poster.Metric
		err := json.Unmarshal(contents, &metrics)
		Expect(err).ToNot(HaveOccurred())

		Expect(logOutput).ToNot(gbytes.Say("Error while reading from the firehose"))

		// +3 internal metrics that show totalMessagesReceived, totalMetricSent, and slowConsumerAlert
		Expect(metrics).To(HaveLen(13))

	}, 2)

	It("does not report slow consumer error when closed for other reasons", func(done Done) {
		defer close(done)

		fakeFirehose.SetCloseMessage(websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "Weird things happened."))

		go nozzle.Start()

		var contents []byte
		Eventually(fakeOpenTSDB.ReceivedContents).Should(Receive(&contents))

		var metrics []poster.Metric
		err := json.Unmarshal(contents, &metrics)
		Expect(err).ToNot(HaveOccurred())

		errMetric := findSlowConsumerMetric(metrics)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Value).To(BeEquivalentTo(0))

		Expect(logOutput).To(gbytes.Say("Error while reading from the firehose"))
		Expect(logOutput).NotTo(gbytes.Say("Client did not respond to ping before keep-alive timeout expired."))
		Expect(logOutput).NotTo(gbytes.Say("Disconnected because nozzle couldn't keep up."))
	}, 2)

	It("gets a valid authentication token", func() {
		go nozzle.Start()
		Eventually(fakeFirehose.Requested).Should(BeTrue())
		Consistently(fakeFirehose.LastAuthorization).Should(Equal("bearer 123456789"))
	})

	Context("receives a truncatingbuffer.droppedmessage value metric,", func() {
		It("sets a slow-consumer error", func() {
			slowConsumerError := events.Envelope{
				Origin:    proto.String("doppler"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_CounterEvent.Enum(),
				CounterEvent: &events.CounterEvent{
					Name:  proto.String("TruncatingBuffer.DroppedMessages"),
					Delta: proto.Uint64(1),
					Total: proto.Uint64(1),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
			}
			fakeFirehose.AddEvent(slowConsumerError)

			go nozzle.Start()

			var contents []byte
			Eventually(fakeOpenTSDB.ReceivedContents).Should(Receive(&contents))

			var metrics []poster.Metric
			err := json.Unmarshal(contents, &metrics)
			Expect(err).ToNot(HaveOccurred())

			Expect(findSlowConsumerMetric(metrics)).NotTo(BeNil())

			Expect(logOutput).To(gbytes.Say("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle."))
		})
	})

	Context("when the DisableAccessControl is set to true", func() {
		var tokenFetcher *FakeTokenFetcher

		BeforeEach(func() {
			fakeUAA = NewFakeUAA("", "")
			fakeToken := fakeUAA.AuthToken()
			fakeFirehose = NewFakeFirehose(fakeToken)
			fakeOpenTSDB = NewFakeOpenTSDB()
			tokenFetcher = &FakeTokenFetcher{}

			fakeUAA.Start()
			fakeFirehose.Start()
			fakeOpenTSDB.Start()

			config = &nozzleconfig.NozzleConfig{
				FlushDurationSeconds: 1,
				OpenTSDBURL:          fakeOpenTSDB.URL(),
				TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: true,
			}

			nozzle = opentsdbfirehosenozzle.NewOpenTSDBFirehoseNozzle(config, tokenFetcher)
		})

		AfterEach(func() {
			fakeUAA.Close()
			fakeFirehose.Close()
			fakeOpenTSDB.Close()
		})

		It("still tries to connect to the firehose", func() {
			go nozzle.Start()
			Eventually(fakeFirehose.Requested).Should(BeTrue())
		})

		It("gets an empty authentication token", func() {
			go nozzle.Start()
			Consistently(fakeUAA.Requested).Should(Equal(false))
			Consistently(fakeFirehose.LastAuthorization).Should(Equal(""))
		})

		It("does not rquire the presence of config.UAAURL", func() {
			nozzle.Start()
			Consistently(func() int { return tokenFetcher.NumCalls }).Should(Equal(0))
		})
	})
})

func findSlowConsumerMetric(metrics []poster.Metric) *poster.Metric {
	for _, metric := range metrics {
		if metric.Metric == "opentsdb.nozzle.slowConsumerAlert" {
			return &metric
		}
	}
	return nil
}
