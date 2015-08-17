package main

import (
	"flag"
	"log"

	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/nozzleconfig"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/opentsdbfirehosenozzle"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/uaatokenfetcher"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"
)

func main() {
	configFilePath := flag.String("config", "config/opentsdb-firehose-nozzle.json", "Location of the nozzle config json file")
	flag.Parse()

	config, err := nozzleconfig.Parse(*configFilePath)
	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	tokenFetcher := &uaatokenfetcher.UAATokenFetcher{
		UaaUrl:                config.UAAURL,
		Username:              config.Username,
		Password:              config.Password,
		InsecureSSLSkipVerify: config.InsecureSSLSkipVerify,
	}

	threadDumpChan := registerGoRoutineDumpSignalChannel()
	defer close(threadDumpChan)
	go dumpGoRoutine(threadDumpChan)

	opentsdbNozzle := opentsdbfirehosenozzle.NewOpenTSDBFirehoseNozzle(config, tokenFetcher)
	opentsdbNozzle.Start()
}

func registerGoRoutineDumpSignalChannel() chan os.Signal {
	threadDumpChan := make(chan os.Signal, 1)
	signal.Notify(threadDumpChan, syscall.SIGUSR1)

	return threadDumpChan
}

func dumpGoRoutine(dumpChan chan os.Signal) {
	for range dumpChan {
		goRoutineProfiles := pprof.Lookup("goroutine")
		if goRoutineProfiles != nil {
			goRoutineProfiles.WriteTo(os.Stdout, 2)
		}
	}
}
