package opentsdbclient_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"io/ioutil"
	"log"
	"testing"
)

func TestOpentsdbclient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Opentsdbclient Suite")
}

var _ = BeforeSuite(func() {
	log.SetOutput(ioutil.Discard)
})
