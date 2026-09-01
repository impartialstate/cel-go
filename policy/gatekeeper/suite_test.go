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
	"testing"
)

// TestRunSuite runs a gator test suite against the CEL of the templates it
// names, with no cluster and no gator.
func TestRunSuite(t *testing.T) {
	RunSuite(t, "testdata/suite/suite.yaml")
}

func TestSuiteResult(t *testing.T) {
	suite, err := ReadSuite("testdata/suite/suite.yaml")
	if err != nil {
		t.Fatalf("ReadSuite() failed: %v", err)
	}
	result, err := suite.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	if !result.Passed() {
		t.Errorf("Run() reported failures:\n%s", result.Report())
	}
	if len(result.Cases) != 6 {
		t.Errorf("Run() ran %d cases, wanted the 6 the suite declares", len(result.Cases))
	}
	report := result.Report()
	if strings.Count(report, "PASS") != 6 {
		t.Errorf("Report() is:\n%s\nwanted a line per passing case", report)
	}
}

// TestSuiteReportsFailures confirms that an assertion which does not hold is
// reported with what the review actually found.
func TestSuiteReportsFailures(t *testing.T) {
	suite, err := ReadSuite("testdata/suite/suite.yaml")
	if err != nil {
		t.Fatalf("ReadSuite() failed: %v", err)
	}
	// Assert the opposite of what the policy reports for an unlabelled pod.
	suite.Tests[0].Cases[1].Assertions = []*Assertion{{Violations: &intOrString{value: "no"}}}
	result, err := suite.Run(context.Background())
	if err != nil {
		t.Fatalf("Run() failed: %v", err)
	}
	if result.Passed() {
		t.Fatal("Run() passed, wanted the altered assertion to fail")
	}
	failed := result.Cases[1]
	if failed.Passed() || len(failed.Failures) != 1 {
		t.Fatalf("case %s/%s got %v, wanted one failure", failed.Test, failed.Case, failed.Failures)
	}
	got := failed.Failures[0].Error()
	if !strings.Contains(got, "wanted no violations") || !strings.Contains(got, "missing required label") {
		t.Errorf("failure is %q, wanted it to name the expectation and what the review found", got)
	}
}
