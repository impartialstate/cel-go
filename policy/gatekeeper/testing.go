// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gatekeeper

import (
	"context"
	"strings"
)

// TestingT is the part of *testing.T which the suite runner reports through.
type TestingT interface {
	Helper()
	Logf(format string, args ...any)
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// RunSuite runs a gator test suite against the CEL of the templates it names,
// reporting each case which does not meet its assertions.
//
//	func TestPolicies(t *testing.T) {
//	    gatekeeper.RunSuite(t, "suite.yaml")
//	}
func RunSuite(t TestingT, path string, opts ...Option) {
	t.Helper()
	suite, err := ReadSuite(path)
	if err != nil {
		t.Fatalf("ReadSuite(%s) failed: %v", path, err)
		return
	}
	result, err := suite.Run(context.Background(), opts...)
	if err != nil {
		t.Fatalf("Run(%s) failed: %v", path, err)
		return
	}
	for _, c := range result.Cases {
		if c.Skipped {
			t.Logf("%s/%s: skipped", c.Test, c.Case)
			continue
		}
		for _, failure := range c.Failures {
			t.Errorf("%s/%s: %v", c.Test, c.Case, failure)
		}
	}
}

// Report renders the outcome of a suite as lines of text, for tools which
// present the result rather than failing a test.
func (r *SuiteResult) Report() string {
	var out strings.Builder
	for _, c := range r.Cases {
		switch {
		case c.Skipped:
			out.WriteString("SKIP " + c.Test + "/" + c.Case + "\n")
		case c.Passed():
			out.WriteString("PASS " + c.Test + "/" + c.Case + "\n")
		default:
			out.WriteString("FAIL " + c.Test + "/" + c.Case + "\n")
			for _, failure := range c.Failures {
				out.WriteString("       " + failure.Error() + "\n")
			}
		}
	}
	return out.String()
}
