package matcher_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/pivotal-cloudops/opentsdb-firehose-nozzle/matcher"
)

var _ = Describe("matchers", func() {
	var beContainedIn *matcher.BeContainedInMatcher
	BeforeEach(func() {
		beContainedIn = matcher.BeContainedIn("a", "b", "c", "2")
	})

	var _ = Describe("Match", func() {
		It("compares actual to each Element", func() {
			for _, elem := range []string{"a", "b", "c"} {
				match, err := beContainedIn.Match(elem)
				Expect(err).ToNot(HaveOccurred())
				Expect(match).To(BeTrue())
			}
		})
		It("returns an error when actual is not contained", func() {
			match, err := beContainedIn.Match("d")
			Expect(err).ToNot(HaveOccurred())
			Expect(match).To(BeFalse())

		})
		It("works if actual is not a string and respects type", func() {
			match, err := beContainedIn.Match(2)
			Expect(err).ToNot(HaveOccurred())
			Expect(match).To(BeFalse())
		})
	})

	var _ = Describe("FailureMessage", func() {

		It("concatonates the failure message of all matchers", func() {
			msg := beContainedIn.FailureMessage("Fake Test Value")

			expectedMessagePattern := `Expected
    <string>: Fake Test Value
to match one of
a, b, c, 2`

			Expect(msg).To(MatchRegexp(expectedMessagePattern))
		})
	})

	var _ = Describe("NegatedFailureMessage", func() {
		It("concatonates the failure message of all matchers", func() {
			msg := beContainedIn.NegatedFailureMessage("Fake Test Value")

			expectedMessagePattern := `Expected
    <string>: Fake Test Value
not to match one of
a, b, c, 2`

			Expect(msg).To(MatchRegexp(expectedMessagePattern))
		})
	})
})
