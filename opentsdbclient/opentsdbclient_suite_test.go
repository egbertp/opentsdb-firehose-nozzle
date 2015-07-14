package opentsdbclient_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestOpentsdbclient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Opentsdbclient Suite")
}
