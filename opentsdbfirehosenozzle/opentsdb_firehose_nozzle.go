package opentsdbfirehosenozzle

import (
	"crypto/tls"
	"log"
	"time"

	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/nozzleconfig"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/opentsdbclient"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/poster"
	"github.com/pivotal-golang/localip"
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

	var transporter opentsdbclient.Poster
	if o.config.UseTelnetAPI {
		transporter = poster.NewTelnetPoster(o.config.OpenTSDBURL)
	} else {
		transporter = poster.NewHTTPPoster(o.config.OpenTSDBURL)
	}
	o.client = opentsdbclient.New(transporter, o.config.MetricPrefix, o.config.Deployment, o.config.Job, o.config.Index, ipAddress)
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
	for {
		select {
		case <-ticker.C:
			o.postMetrics()
		case envelope := <-o.messages:
			o.handleMessage(envelope)
			o.client.AddMetric(envelope)
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
	switch closeErr := err.(type) {
	case *websocket.CloseError:
		switch closeErr.Code {
		case websocket.CloseNormalClosure:
			// no op
		case websocket.ClosePolicyViolation:
			log.Printf("Error while reading from the firehose: %v", err)
			log.Printf("Disconnected because nozzle couldn't keep up. Please try scaling up the nozzle.")
			o.client.AlertSlowConsumerError()
		default:
			log.Printf("Error while reading from the firehose: %v", err)
		}
	default:
		log.Printf("Error while reading from the firehose: %v", err)
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
