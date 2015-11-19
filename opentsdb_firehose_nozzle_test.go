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

	"fmt"
	"net"
	"strings"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/poster"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/util"
)

var (
	fakeFirehoseInputChan chan *events.Envelope
	fakeOpenTSDBChan      chan []byte
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
		fakeOpenTSDBChan = make(chan []byte)

		fakeUAA = &http.Server{
			Addr:    ":8084",
			Handler: http.HandlerFunc(fakeUAAHandler),
		}
		fakeFirehose = &http.Server{
			Addr:    ":8086",
			Handler: http.HandlerFunc(fakeFirehoseHandler),
		}

		go fakeUAA.ListenAndServe()
		go fakeFirehose.ListenAndServe()

	})

	AfterEach(func() {
		nozzleSession.Kill().Wait()
	})

	Context("with an HTTP OpenTSDB endpoint", func() {
		BeforeEach(func() {
			fakeOpentsdb = &http.Server{
				Addr:    ":8087",
				Handler: http.HandlerFunc(fakeOpentsdbHandler),
			}
			go fakeOpentsdb.ListenAndServe()

			var err error
			nozzleCommand := exec.Command(pathToNozzleExecutable, "-config", "fixtures/http-test-config.json")
			nozzleSession, err = gexec.Start(
				nozzleCommand,
				gexec.NewPrefixedWriter("[o][nozzle] ", GinkgoWriter),
				gexec.NewPrefixedWriter("[e][nozzle] ", GinkgoWriter),
			)
			Expect(err).NotTo(HaveOccurred())
		})
		It("forwards metrics in a batch", func(done Done) {
			sendEventsThroughFirehose(fakeFirehoseInputChan)

			// eventually receive a batch from fake DD
			var messageBytes []byte
			Eventually(fakeOpenTSDBChan, "2s").Should(Receive(&messageBytes))

			// Break JSON blob into a list of blobs, one for each metric
			var metrics []poster.Metric

			log.Printf("Received message is: %s\n", string(messageBytes))
			err := json.Unmarshal(util.UnzipIgnoreError(messageBytes), &metrics)
			Expect(err).NotTo(HaveOccurred())

			Expect(metrics).To(ContainElement(
				poster.Metric{
					Metric:    "origin.metricName",
					Timestamp: 1,
					Value:     5,
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
					Timestamp: 2,
					Value:     10,
					Tags: poster.Tags{
						Deployment: "deployment-name",
						Job:        "gorouter",
						Index:      0,
						IP:         "",
					},
				}))
			Expect(metrics).To(ContainElement(
				poster.Metric{
					Metric:    "origin.counterName",
					Timestamp: 3,
					Value:     15,
					Tags: poster.Tags{
						Deployment: "deployment-name",
						Job:        "doppler",
						Index:      0,
						IP:         "",
					},
				}))

			close(done)
		}, 2.0)
	})

	Context("with a Telnet OpenTSDB endpoint", func() {
		var telnetListener *net.TCPListener
		BeforeEach(func() {
			telnetListener = fakeOpenTSDBTelnetListener()
			var err error
			nozzleCommand := exec.Command(pathToNozzleExecutable, "-config", "fixtures/telnet-test-config.json")
			nozzleSession, err = gexec.Start(
				nozzleCommand,
				gexec.NewPrefixedWriter("[o][nozzle] ", GinkgoWriter),
				gexec.NewPrefixedWriter("[e][nozzle] ", GinkgoWriter),
			)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			telnetListener.Close()
		})

		It("forwards metrics in a batch", func(done Done) {
			sendEventsThroughFirehose(fakeFirehoseInputChan)

			var receivedBytes []byte
			Eventually(fakeOpenTSDBChan).Should(Receive(&receivedBytes))
			receivedMetrics := strings.Split(string(receivedBytes), "\n")
			Expect(receivedMetrics).To(HaveLen(7))
			Expect(receivedMetrics).To(ContainElement(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=0 job=doppler", 1, 5.0)))
			Expect(receivedMetrics).To(ContainElement(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=0 job=gorouter", 2, 10.0)))
			Expect(receivedMetrics).To(ContainElement(fmt.Sprintf("put origin.counterName %d %f deployment=deployment-name index=0 job=doppler", 3, 15.0)))

			close(done)
		}, 2.0)

	})

})

func sendEventsThroughFirehose(fakeFirehoseInputChan chan *events.Envelope) {
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
}

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
		fakeOpenTSDBChan <- contents
	}()
}

func fakeOpenTSDBTelnetListener() *net.TCPListener {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:8088")
	if err != nil {
		panic(err)
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			// Listen for an incoming connection.
			conn, err := listener.Accept()

			if err != nil || conn == nil {
				log.Println("Error accepting: ", err.Error())
				return
			}

			// Handle connections in a new goroutine.
			handleRequest(conn)
		}
	}()

	return listener

}

// Handles incoming requests.
func handleRequest(conn net.Conn) {

	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)
	// Read the incoming connection into the buffer.
	if conn != nil {
		_, err := conn.Read(buf)
		if err != nil {
			log.Println("Error reading:", err.Error())
			conn.Close()
			return
		}

		fakeOpenTSDBChan <- buf

		// Close the connection when you're done with it.
		conn.Close()
	}

}
