package main_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"

	"github.com/onsi/gomega/gexec"
)

func TestOpentsdbFirehoseNozzle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpentsdbFirehoseNozzle Suite")
}

var pathToNozzleExecutable string = "./opentsdb-firehose-nozzle"

var _ = BeforeSuite(func() {
})

var _ = AfterSuite(func() {
	gexec.CleanupBuildArtifacts()
})
