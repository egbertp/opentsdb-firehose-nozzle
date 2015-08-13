package opentsdbfirehosenozzle_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"io/ioutil"
	"log"
	"testing"
)

func TestOpentsdbfirehosenozzle(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "OpenTSDBFirehoseNozzle Suite")
}

var _ = BeforeSuite(func() {
	log.SetOutput(ioutil.Discard)
})
