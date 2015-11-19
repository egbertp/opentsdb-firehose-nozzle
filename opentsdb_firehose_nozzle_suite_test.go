package main_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"

	"io/ioutil"
	"log"

	"github.com/onsi/gomega/gexec"
)

func TestOpentsdbFirehoseNozzle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpentsdbFirehoseNozzle Suite")
}

var pathToNozzleExecutable string

var _ = BeforeSuite(func() {
	var err error
	pathToNozzleExecutable, err = gexec.Build("github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle")
	Expect(err).ShouldNot(HaveOccurred())

	log.SetOutput(ioutil.Discard)
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})
