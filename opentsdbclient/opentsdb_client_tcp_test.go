package opentsdbclient_test

import (
	"fmt"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gogo/protobuf/proto"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/opentsdbclient"
	"log"
	"net"
	"strings"
	"time"
)

var telnetChan chan []byte

var _ = Describe("OpentsdbClient Tcp", func() {

	var c *opentsdbclient.Client
	var tcpListener *net.TCPListener

	BeforeEach(func() {
		telnetChan = make(chan []byte, 1)
		tcpListener = NewTCPServer()
		poster := opentsdbclient.NewTelnetPoster(tcpListener.Addr().String())
		c = opentsdbclient.New(poster, "", "test-deployment", "dummy-ip")
	})

	It("posts value metrics over tcp", func() {
		timestamp := time.Now()
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(timestamp.UnixNano()),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Job:        proto.String("doppler"),
			Index:      proto.String("0"),
			Ip:         proto.String("10.10.10.10"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		receivedMetrics := strings.Split(string(receivedBytes), "\n")
		Expect(receivedMetrics).To(HaveLen(5)) // 1 metric + 3 internal metrics + extra newline at end of last metric = 5
		Expect(receivedMetrics).To(ContainElement(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=0 ip=10.10.10.10 job=doppler", timestamp.Unix(), 5.0)))
	})

	It("ignores missing deployment tag", func() {
		timestamp := time.Now()
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(timestamp.UnixNano()),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Job:   proto.String("doppler"),
			Index: proto.String("0"),
			Ip:    proto.String("10.10.10.10"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		Expect(string(receivedBytes)).To(ContainSubstring(fmt.Sprintf("put origin.metricName %d %f index=0 ip=10.10.10.10 job=doppler\n", timestamp.Unix(), 5.0)))
	})

	It("ignores missing job tag", func() {
		timestamp := time.Now()
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(timestamp.UnixNano()),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Deployment: proto.String("deployment-name"),
			Index:      proto.String("0"),
			Ip:         proto.String("10.10.10.10"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		Expect(string(receivedBytes)).To(ContainSubstring(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=0 ip=10.10.10.10\n", timestamp.Unix(), 5.0)))
	})

	It("ignores missing ip tag", func() {
		timestamp := time.Now()
		c.AddMetric(&events.Envelope{
			Origin:    proto.String("origin"),
			Timestamp: proto.Int64(timestamp.UnixNano()),
			EventType: events.Envelope_ValueMetric.Enum(),
			ValueMetric: &events.ValueMetric{
				Name:  proto.String("metricName"),
				Value: proto.Float64(5),
			},
			Job:        proto.String("doppler"),
			Deployment: proto.String("deployment-name"),
			Index:      proto.String("0"),
		})

		err := c.PostMetrics()
		Expect(err).ToNot(HaveOccurred())

		var receivedBytes []byte
		Eventually(telnetChan).Should(Receive(&receivedBytes))
		Expect(string(receivedBytes)).To(ContainSubstring(fmt.Sprintf("put origin.metricName %d %f deployment=deployment-name index=0 job=doppler\n", timestamp.Unix(), 5.0)))
	})

	It("shows a proper error when the connection does not work", func() {
		address := tcpListener.Addr().String()
		tcpListener.Close()

		err := c.PostMetrics()
		Expect(err).Should(MatchError(fmt.Sprintf("dial tcp %s: connection refused", address)))
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
