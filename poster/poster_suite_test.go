package poster_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"io/ioutil"
	"log"
	"testing"
)

func TestPoster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Poster Suite")
}

var _ = BeforeSuite(func() {
	log.SetOutput(ioutil.Discard)
})
