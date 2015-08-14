package opentsdbfirehosenozzle

import (
	"crypto/tls"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/nozzleconfig"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/opentsdbclient"
	"github.com/pivotal-golang/localip"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
)

type OpenTSDBFirehoseNozzle struct {
	config           *nozzleconfig.NozzleConfig
	errs             chan error
	messages         chan *events.Envelope
	authTokenFetcher AuthTokenFetcher
	consumer         *noaa.Consumer
	client           *opentsdbclient.Client
}

type AuthTokenFetcher interface {
	FetchAuthToken() string
}

func NewOpenTSDBFirehoseNozzle(config *nozzleconfig.NozzleConfig, tokenFetcher AuthTokenFetcher) *OpenTSDBFirehoseNozzle {
	return &OpenTSDBFirehoseNozzle{
		config:           config,
		errs:             make(chan error),
		messages:         make(chan *events.Envelope),
		authTokenFetcher: tokenFetcher,
	}
}

func (o *OpenTSDBFirehoseNozzle) Start() {
	var authToken string

	if !o.config.DisableAccessControl {
		authToken = o.authTokenFetcher.FetchAuthToken()
	}

	log.Print("Starting OpenTSDB Firehose Nozzle...")
	o.createClient()
	go o.consumeFirehose(authToken)
	o.postToOpenTSDB()
	log.Print("OpenTSDB Firehose Nozzle shutting down...")
}

func (o *OpenTSDBFirehoseNozzle) createClient() {
	ipAddress, err := localip.LocalIP()
	if err != nil {
		panic(err)
	}

	o.client = opentsdbclient.New(o.config.OpenTSDBURL, o.config.MetricPrefix, o.config.Deployment, ipAddress)
}

func (o *OpenTSDBFirehoseNozzle) consumeFirehose(authToken string) {
	o.consumer = noaa.NewConsumer(
		o.config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: o.config.InsecureSSLSkipVerify},
		nil)
	o.consumer.Firehose(o.config.FirehoseSubscriptionID, authToken, o.messages, o.errs)
}

func (o *OpenTSDBFirehoseNozzle) postToOpenTSDB() {
	ticker := time.NewTicker(time.Duration(o.config.FlushDurationSeconds) * time.Second)
	count := 0
	for {
		select {
		case <-ticker.C:
			o.postMetrics()
			count = 0
		case envelope := <-o.messages:
			count++
			o.handleMessage(envelope)
			o.client.AddMetric(envelope)

			if count > 50 {
				o.postMetrics()
				count = 0
			}
		case err := <-o.errs:
			o.handleError(err)
			o.postMetrics()
			return
		}
	}
}

func (o *OpenTSDBFirehoseNozzle) postMetrics() {
	err := o.client.PostMetrics()
	if err != nil {
		log.Printf("Error: %s", err.Error())
	}
}

func (o *OpenTSDBFirehoseNozzle) handleError(err error) {
	if err != io.EOF {
		log.Printf("Error while reading from the firehose: %v", err)
	}

	if isCloseError(err) {
		log.Printf("Disconnected because nozzle couldn't keep up. Please try scaling up the nozzle.")
		o.client.AlertSlowConsumerError()
	}

	log.Printf("Closing connection with traffic controller due to %v", err)
	o.consumer.Close()
}

func (d *OpenTSDBFirehoseNozzle) handleMessage(envelope *events.Envelope) {
	if envelope.GetEventType() == events.Envelope_CounterEvent && envelope.CounterEvent.GetName() == "TruncatingBuffer.DroppedMessages" && envelope.GetOrigin() == "doppler" {
		log.Printf("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle.")
		d.client.AlertSlowConsumerError()
	}
}

func isCloseError(err error) bool {
	errorMsg := "websocket: close " + strconv.Itoa(websocket.CloseInternalServerErr)
	return strings.Contains(err.Error(), errorMsg)
}
