package opentsdbfirehosenozzle

import (
	"crypto/tls"
	"log"
	"time"

	"github.com/cloudfoundry/noaa/consumer"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/nozzleconfig"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/opentsdbclient"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/poster"
	"github.com/pivotal-golang/localip"
)

type OpenTSDBFirehoseNozzle struct {
	config           *nozzleconfig.NozzleConfig
	errs             <-chan error
	messages         <-chan *events.Envelope
	authTokenFetcher AuthTokenFetcher
	consumer         *consumer.Consumer
	client           *opentsdbclient.Client
}

type AuthTokenFetcher interface {
	FetchAuthToken() string
}

func NewOpenTSDBFirehoseNozzle(config *nozzleconfig.NozzleConfig, tokenFetcher AuthTokenFetcher) *OpenTSDBFirehoseNozzle {
	return &OpenTSDBFirehoseNozzle{
		config:           config,
		errs:             make(<-chan error),
		messages:         make(<-chan *events.Envelope),
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
	o.consumeFirehose(authToken)
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
	o.consumer = consumer.New(
		o.config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: o.config.InsecureSSLSkipVerify},
		nil)
	o.consumer.SetIdleTimeout(time.Duration(o.config.IdleTimeoutSeconds) * time.Second)
	o.messages, o.errs = o.consumer.Firehose(o.config.FirehoseSubscriptionID, authToken)
}

func (o *OpenTSDBFirehoseNozzle) postToOpenTSDB() {
	ticker := time.NewTicker(time.Duration(o.config.FlushDurationSeconds) * time.Second)
	for {
		select {
		case <-ticker.C:
			o.postMetrics()
		case envelope := <-o.messages:
			o.client.AddMetric(envelope)
		case err := <-o.errs:
			o.handleError(err)
			o.postMetrics()
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
	o.client.IncrementFirehoseDisconnect()
	log.Printf("Closing connection with traffic controller due to %v", err)
	o.consumer.Close()

	time.Sleep(10 * time.Second)

	log.Println("Reconnecting to Firehose")
	if o.config.DisableAccessControl {
		o.consumeFirehose("")
		return
	}
	o.consumeFirehose(o.authTokenFetcher.FetchAuthToken())
}
