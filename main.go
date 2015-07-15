package main

import (
	"fmt"

	"crypto/tls"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"time"

	"./opentsdbclient"
	"github.com/cloudfoundry-incubator/uaago"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/sonde-go/events"
)

type nozzleConfig struct {
	UAAURL                 string
	Username               string
	Password               string
	TrafficControllerURL   string
	FirehoseSubscriptionID string
	OpentsdbURL            string
	FlushDurationSeconds   uint32
	InsecureSSLSkipVerify  bool
	MetricPrefix           string
}

func main() {
	var (
		configFilePath = flag.String("config", "config/opentsdb-firehose-nozzle.json", "Location of the nozzle config json file")
	)
	flag.Parse()
	config, err := parseConfig(*configFilePath)

	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	uaaClient, err := uaago.NewClient(config.UAAURL)
	if err != nil {
		log.Fatalf("Error creating uaa client: %s", err.Error())
	}

	authToken, err := uaaClient.GetAuthToken(config.Username, config.Password, config.InsecureSSLSkipVerify)
	if err != nil {
		log.Fatalf("Error getting oauth token: %s. Please check your username and password.", err.Error())
	}

	consumer := noaa.NewConsumer(
		config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: config.InsecureSSLSkipVerify},
		nil)
	messages := make(chan *events.Envelope)
	errs := make(chan error)
	done := make(chan struct{})
	go consumer.Firehose(config.FirehoseSubscriptionID, authToken, messages, errs)

	go func() {
		err := <-errs
		log.Printf("Error while reading from the firehose: %s", err.Error())
		close(done)
	}()

	client := opentsdbclient.New(config.OpentsdbURL, config.MetricPrefix)
	ticker := time.NewTicker(time.Duration(config.FlushDurationSeconds) * time.Second)

	for {
		select {
		case <-ticker.C:
			postMetrics(client)
		case envelope := <-messages:
			client.AddMetric(envelope)
		case <-done:
			postMetrics(client)
			consumer.Close()
			return
		}
	}
}

func parseConfig(configPath string) (nozzleConfig, error) {
	configBytes, err := ioutil.ReadFile(configPath)
	var config nozzleConfig
	if err != nil {
		return config, fmt.Errorf("Can not read config file [%s]: %s", configPath, err)
	}

	err = json.Unmarshal(configBytes, &config)
	if err != nil {
		return config, fmt.Errorf("Can not parse config file %s: %s", configPath, err)
	}
	return config, err
}

func postMetrics(client *opentsdbclient.Client) {
	err := client.PostMetrics()
	if err != nil {
		log.Printf("Error: %s", err.Error())
	}
}
