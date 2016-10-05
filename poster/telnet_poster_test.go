package poster_test

import (
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/poster"
)

var telnetChan chan []byte

var _ = Describe("OpentsdbClient Tcp", func() {

	var tcpListener *net.TCPListener
	var p *poster.TelnetPoster

	BeforeEach(func() {
		telnetChan = make(chan []byte, 1)
		tcpListener = NewTCPServer()
		p = poster.NewTelnetPoster(tcpListener.Addr().String())

	})

	It("posts value metrics over tcp", func() {
		timestamp := time.Now().Unix()
		metric := poster.Metric{
			Metric:    "origin.metricName",
			Value:     5,
			Timestamp: timestamp,
			Tags: poster.Tags{
				Deployment: "deployment-name",
				Job:        "doppler",
				Index:      "SOME-GUID",
				IP:         "10.10.10.10",
			},
		}

		err := p.Post([]poster.Metric{metric})
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		receivedMetrics := strings.Split(string(receivedBytes), "\n")
		Expect(receivedMetrics).To(HaveLen(2)) // 1 metric + extra newline at end of last metric = 2
		Expect(receivedMetrics).To(ContainElement(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=SOME-GUID ip=10.10.10.10 job=doppler", timestamp, 5.0)))
	})

	It("ignores missing deployment tag", func() {
		timestamp := time.Now().Unix()
		metric := poster.Metric{
			Metric:    "origin.metricName",
			Value:     5,
			Timestamp: timestamp,
			Tags: poster.Tags{
				Job:   "doppler",
				Index: "SOME-GUID",
				IP:    "10.10.10.10",
			},
		}

		err := p.Post([]poster.Metric{metric})
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		Expect(string(receivedBytes)).To(ContainSubstring(fmt.Sprintf("put origin.metricName %d %f index=SOME-GUID ip=10.10.10.10 job=doppler\n", timestamp, 5.0)))
	})

	It("ignores missing job tag", func() {
		timestamp := time.Now().Unix()
		metric := poster.Metric{
			Metric:    "origin.metricName",
			Value:     5,
			Timestamp: timestamp,
			Tags: poster.Tags{
				Deployment: "deployment-name",
				Index:      "SOME-GUID",
				IP:         "10.10.10.10",
			},
		}

		err := p.Post([]poster.Metric{metric})
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		Expect(string(receivedBytes)).To(ContainSubstring(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=SOME-GUID ip=10.10.10.10\n", timestamp, 5.0)))
	})

	It("ignores missing ip tag", func() {
		timestamp := time.Now().Unix()
		metric := poster.Metric{
			Metric:    "origin.metricName",
			Value:     5,
			Timestamp: timestamp,
			Tags: poster.Tags{
				Deployment: "deployment-name",
				Job:        "doppler",
				Index:      "SOME-GUID",
			},
		}

		err := p.Post([]poster.Metric{metric})
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		Expect(string(receivedBytes)).To(ContainSubstring(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=SOME-GUID job=doppler\n", timestamp, 5.0)))
	})

	It("shows a proper error when the connection does not work", func() {
		address := tcpListener.Addr().String()
		tcpListener.Close()

		Eventually(func() error { return p.Post([]poster.Metric{}) }).Should(MatchError(fmt.Sprintf("dial tcp %s: getsockopt: connection refused", address)))
	})

})

func NewTCPServer() *net.TCPListener {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
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

		telnetChan <- buf

		// Close the connection when you're done with it.
		conn.Close()
	}

}
