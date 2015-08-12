package main_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"

	"github.com/onsi/gomega/gexec"
	"io/ioutil"
	"log"
)

func TestOpentsdbFirehoseNozzle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpentsdbFirehoseNozzle Suite")
}

var pathToNozzleExecutable string

var _ = BeforeSuite(func() {
	var err error
	pathToNozzleExecutable, err = gexec.Build("github.com/pivotal-cloudops/opentsdb-firehose-nozzle")
	Expect(err).ShouldNot(HaveOccurred())

	log.SetOutput(ioutil.Discard)
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})
