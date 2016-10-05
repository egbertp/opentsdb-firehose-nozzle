package nozzleconfig_test

import (
	"os"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cf-experimental/opentsdb-firehose-nozzle/nozzleconfig"
)

var _ = Describe("NozzleConfig", func() {
	BeforeEach(func() {
		os.Clearenv()
	})

	It("successfully parses a valid config", func() {
		conf, err := nozzleconfig.Parse("../config/opentsdb-firehose-nozzle.json")
		Expect(err).ToNot(HaveOccurred())
		Expect(conf.UAAURL).To(Equal("https://uaa.pilsner.pcf-metrics.com"))
		Expect(conf.Username).To(Equal("apps_metrics_processing"))
		Expect(conf.Password).To(Equal("secret"))
		Expect(conf.TrafficControllerURL).To(Equal("wss://doppler.pilsner.pcf-metrics.com:4443"))
		Expect(conf.FirehoseSubscriptionID).To(Equal("opentsdb-nozzle"))
		Expect(conf.OpenTSDBURL).To(Equal("http://localhost"))
		Expect(conf.FlushDurationSeconds).To(BeEquivalentTo(15))
		Expect(conf.InsecureSSLSkipVerify).To(Equal(true))
		Expect(conf.MetricPrefix).To(Equal("opentsdbclient"))
		Expect(conf.Deployment).To(Equal("deployment-name"))
		Expect(conf.DisableAccessControl).To(Equal(false))
		Expect(conf.UseTelnetAPI).To(BeEquivalentTo(true))
		Expect(conf.Job).To(Equal("opentsdb-firehose-nozzle"))
		Expect(conf.Index).To(BeEquivalentTo("SOME-GUID"))
		Expect(conf.IdleTimeoutSeconds).To(BeEquivalentTo(60))
	})

	It("successfully overwrites file config values with environmental variables", func() {
		os.Setenv("NOZZLE_UAAURL", "https://uaa.walnut-env.cf-app.com")
		os.Setenv("NOZZLE_USERNAME", "env-user")
		os.Setenv("NOZZLE_PASSWORD", "env-user-password")
		os.Setenv("NOZZLE_TRAFFICCONTROLLERURL", "wss://doppler.walnut-env.cf-app.com:4443")
		os.Setenv("NOZZLE_FIREHOSESUBSCRIPTIONID", "env-opentsdb-nozzle")
		os.Setenv("NOZZLE_OPENTSDBURL", "http://10.10.10.10")
		os.Setenv("NOZZLE_FLUSHDURATIONSECONDS", "25")
		os.Setenv("NOZZLE_INSECURESSLSKIPVERIFY", "false")
		os.Setenv("NOZZLE_METRICPREFIX", "env-opentsdbclient")
		os.Setenv("NOZZLE_DEPLOYMENT", "env-deployment-name")
		os.Setenv("NOZZLE_DISABLEACCESSCONTROL", "true")
		os.Setenv("NOZZLE_USETELNETAPI", "false")
		os.Setenv("NOZZLE_JOB", "env-opentsdb-firehose-nozzle")
		os.Setenv("NOZZLE_INDEX", "SOME-GUID-2")
		os.Setenv("NOZZLE_IDLETIMEOUTSECONDS", "50")

		conf, err := nozzleconfig.Parse("../config/opentsdb-firehose-nozzle.json")
		Expect(err).ToNot(HaveOccurred())
		Expect(conf.UAAURL).To(Equal("https://uaa.walnut-env.cf-app.com"))
		Expect(conf.Username).To(Equal("env-user"))
		Expect(conf.Password).To(Equal("env-user-password"))
		Expect(conf.TrafficControllerURL).To(Equal("wss://doppler.walnut-env.cf-app.com:4443"))
		Expect(conf.FirehoseSubscriptionID).To(Equal("env-opentsdb-nozzle"))
		Expect(conf.OpenTSDBURL).To(Equal("http://10.10.10.10"))
		Expect(conf.FlushDurationSeconds).To(BeEquivalentTo(25))
		Expect(conf.InsecureSSLSkipVerify).To(Equal(false))
		Expect(conf.MetricPrefix).To(Equal("env-opentsdbclient"))
		Expect(conf.Deployment).To(Equal("env-deployment-name"))
		Expect(conf.DisableAccessControl).To(Equal(true))
		Expect(conf.UseTelnetAPI).To(Equal(false))
		Expect(conf.Job).To(Equal("env-opentsdb-firehose-nozzle"))
		Expect(conf.Index).To(BeEquivalentTo("SOME-GUID-2"))
		Expect(conf.IdleTimeoutSeconds).To(BeEquivalentTo(50))
	})
})
