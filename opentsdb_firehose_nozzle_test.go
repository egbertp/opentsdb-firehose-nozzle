package main_test

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os/exec"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"

	"log"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

var (
	fakeFirehoseInputChan chan *events.Envelope
	fakeDDChan            chan []byte
)

var _ = Describe("OpentsdbFirehoseNozzle", func() {
	var (
		fakeUAA      *http.Server
		fakeFirehose *http.Server
		fakeOpentsdb *http.Server

		nozzleSession *gexec.Session
	)

	BeforeEach(func() {
		fakeFirehoseInputChan = make(chan *events.Envelope)
		fakeDDChan = make(chan []byte)

		fakeUAA = &http.Server{
			Addr:    ":8084",
			Handler: http.HandlerFunc(fakeUAAHandler),
		}
		fakeFirehose = &http.Server{
			Addr:    ":8086",
			Handler: http.HandlerFunc(fakeFirehoseHandler),
		}
		fakeOpentsdb = &http.Server{
			Addr:    ":8087",
			Handler: http.HandlerFunc(fakeOpentsdbHandler),
		}

		go fakeUAA.ListenAndServe()
		go fakeFirehose.ListenAndServe()
		go fakeOpentsdb.ListenAndServe()

		var err error
		nozzleCommand := exec.Command(pathToNozzleExecutable, "-config", "fixtures/test-config.json")
		nozzleSession, err = gexec.Start(
			nozzleCommand,
			gexec.NewPrefixedWriter("[o][nozzle] ", GinkgoWriter),
			gexec.NewPrefixedWriter("[e][nozzle] ", GinkgoWriter),
		)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		nozzleSession.Kill().Wait()
	})

	It("forwards metrics in a batch", func(done Done) {
		fakeFirehoseInputChan <- &events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(1000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
				Unit:  proto.String("gauge"),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		}

		fakeFirehoseInputChan <- &events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(2000000000),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(10),
				Unit:  proto.String("gauge"),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("gorouter"),
		}

		fakeFirehoseInputChan <- &events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(3000000000),
			EventType: events.Envelope_CounterEvent.Enum(),
			CounterEvent: &events.CounterEvent{
				Name:  proto.String("counterName"),
				Delta: proto.Uint64(3),
				Total: proto.Uint64(15),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
		}

		close(fakeFirehoseInputChan)

		// eventually receive a batch from fake DD
		var messageBytes []byte
		Eventually(fakeDDChan, "2s").Should(Receive(&messageBytes))

		// Break JSON blob into a list of blobs, one for each metric
		var jsonBlob []interface{}

    log.Printf("Received message is: \n %s\n", string(messageBytes))
		err := json.Unmarshal(messageBytes, &jsonBlob)
		Expect(err).NotTo(HaveOccurred())
		var series [][]byte

		for _, metric := range jsonBlob {
			buffer, _ := json.Marshal(metric)
			series = append(series, buffer)
		}

		Expect(series).To(ConsistOf(
			MatchJSON(`{
                "metric":"origin.metricName",
                "timestamp": 1,
                "value": 5,
                "tags":{"deployment":"deployment-name", "job":"doppler", "index":0, "ip":""}
            }`),
			MatchJSON(`{
                "metric":"origin.metricName",
                "timestamp": 2,
                "value": 10,
                "tags":{"deployment":"deployment-name", "job":"gorouter", "index":0, "ip":""}
            }`),
			MatchJSON(`{
                "metric":"origin.counterName",
                "timestamp": 3,
                "value": 15,
                "tags":{"deployment":"deployment-name", "job":"doppler", "index":0, "ip":""}
            }`),
		))

		close(done)
	}, 2.0)
})

func fakeUAAHandler(rw http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	rw.Write([]byte(`
		{
			"token_type": "bearer",
			"access_token": "good-token"
		}
	`))
}

func fakeFirehoseHandler(rw http.ResponseWriter, r *http.Request) {
	defer GinkgoRecover()
	authorization := r.Header.Get("Authorization")

	if authorization != "bearer good-token" {
		log.Printf("Bad token passed to firehose: %s", authorization)
		rw.WriteHeader(403)
		r.Body.Close()
		return
	}

	upgrader := websocket.Upgrader{
		CheckOrigin: func(*http.Request) bool { return true },
	}

	ws, _ := upgrader.Upgrade(rw, r, nil)

	defer ws.Close()
	defer ws.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Time{})

	for envelope := range fakeFirehoseInputChan {
		buffer, err := proto.Marshal(envelope)
		Expect(err).NotTo(HaveOccurred())
		err = ws.WriteMessage(websocket.BinaryMessage, buffer)
		Expect(err).NotTo(HaveOccurred())
	}
}

func fakeOpentsdbHandler(rw http.ResponseWriter, r *http.Request) {
	contents, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	go func() {
		fakeDDChan <- contents
	}()
}
