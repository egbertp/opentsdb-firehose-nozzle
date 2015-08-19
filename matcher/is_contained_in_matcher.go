package matcher

import (
	"fmt"
	"strings"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
)

type BeContainedInMatcher struct {
	elements []string
}

func BeContainedIn(expected ...string) *BeContainedInMatcher {
	return &BeContainedInMatcher{
		elements: expected,
	}
}

func (m *BeContainedInMatcher) Match(actual interface{}) (success bool, err error) {
	for _, value := range m.elements {
		submatcher := gomega.Equal(value)
		success, err = submatcher.Match(actual)
		if success || err != nil {
			return
		}
	}
	return
}

func (m *BeContainedInMatcher) FailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n%s\n%s\n%s", format.Object(actual, 1), "to match one of", m.expectedValues())
}

func (m *BeContainedInMatcher) NegatedFailureMessage(actual interface{}) (message string) {
	return fmt.Sprintf("Expected\n%s\n%s\n%s", format.Object(actual, 1), "not to match one of", m.expectedValues())
}

func (m *BeContainedInMatcher) expectedValues() string {
	return strings.Join(m.elements, ", ")
}
