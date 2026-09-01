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
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Suite is a set of policy tests in the format Gatekeeper's gator tool reads,
// so that the suites a repository already keeps can be run against the CEL of
// its templates without a cluster.
//
//	kind: Suite
//	apiVersion: test.gatekeeper.sh/v1alpha1
//	tests:
//	  - name: required-labels
//	    template: template.yaml
//	    constraint: constraint.yaml
//	    cases:
//	      - name: labelled
//	        object: samples/labelled.yaml
//	        assertions:
//	          - violations: no
//	      - name: unlabelled
//	        object: samples/unlabelled.yaml
//	        assertions:
//	          - violations: yes
//	            message: "missing required label"
type Suite struct {
	// APIVersion and Kind identify the suite document.
	APIVersion string `yaml:"apiVersion"`
	Kind       string `yaml:"kind"`

	// Tests are the templates under test, each with the cases it is reviewed
	// against.
	Tests []*SuiteTest `yaml:"tests"`

	// dir is the directory the suite was read from, which the paths within it
	// are relative to.
	dir string
}

// SuiteTest reviews one ConstraintTemplate, instantiated by one constraint,
// against a set of cases.
type SuiteTest struct {
	// Name identifies the test.
	Name string `yaml:"name"`

	// Template is the path of the ConstraintTemplate under test.
	Template string `yaml:"template"`

	// Constraint is the path of the constraint which instantiates the template,
	// and supplies the parameters the policy reads.
	Constraint string `yaml:"constraint"`

	// Cases are the objects the template is reviewed against.
	Cases []*SuiteCase `yaml:"cases"`

	// Skip omits the test.
	Skip bool `yaml:"skip"`
}

// SuiteCase is one object reviewed against a template, and what the review is
// expected to report.
type SuiteCase struct {
	// Name identifies the case.
	Name string `yaml:"name"`

	// Object is the path of the object under review. The file holds either a
	// Kubernetes object or an AdmissionReview, which also supplies the old
	// object and the request.
	Object string `yaml:"object"`

	// Inventory is the paths of the objects which make up the cluster the
	// policy reads, for a template which makes referential lookups.
	Inventory []string `yaml:"inventory"`

	// Assertions are the expectations the review must meet. A case with no
	// assertion expects no violation.
	Assertions []*Assertion `yaml:"assertions"`

	// Skip omits the case.
	Skip bool `yaml:"skip"`
}

// Assertion is an expectation about the violations a review reports.
type Assertion struct {
	// Violations is how many violations must match: a count, "yes" for at least
	// one, or "no" for none. It defaults to "yes".
	Violations *intOrString `yaml:"violations"`

	// Message is the text a matching violation must contain. When it is unset,
	// every violation matches.
	Message string `yaml:"message"`
}

// intOrString holds a YAML value which is either a count or one of the words
// gator accepts in its place.
type intOrString struct {
	value string
}

// UnmarshalYAML reads a count or a word.
func (v *intOrString) UnmarshalYAML(node *yaml.Node) error {
	v.value = node.Value
	return nil
}

// String returns the value as it was written.
func (v *intOrString) String() string {
	if v == nil {
		return "yes"
	}
	return v.value
}

// matches reports whether a number of violations satisfies the assertion.
func (v *intOrString) matches(count int) bool {
	switch strings.ToLower(v.String()) {
	case "yes", "true":
		return count > 0
	case "no", "false":
		return count == 0
	default:
		want, err := strconv.Atoi(v.String())
		if err != nil {
			return false
		}
		return count == want
	}
}

// ReadSuite reads a test suite, resolving the paths within it relative to the
// suite's own directory.
func ReadSuite(path string) (*Suite, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	suite := &Suite{}
	if err := yaml.Unmarshal(content, suite); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if suite.Kind != "" && suite.Kind != "Suite" {
		return nil, fmt.Errorf("%s: unsupported kind: %s, wanted Suite", path, suite.Kind)
	}
	suite.dir = filepath.Dir(path)
	return suite, nil
}

// CaseResult is the outcome of reviewing one object against a template.
type CaseResult struct {
	// Test and Case name the case within the suite.
	Test string
	Case string

	// Skipped reports a case the suite omits.
	Skipped bool

	// Violations are the messages the review reported.
	Violations []string

	// Failures are the assertions the review did not meet, and the errors which
	// prevented it from being made at all.
	Failures []error
}

// Passed reports whether the case met every assertion.
func (r *CaseResult) Passed() bool {
	return len(r.Failures) == 0
}

// SuiteResult is the outcome of running a suite.
type SuiteResult struct {
	Cases []*CaseResult
}

// Passed reports whether every case met its assertions.
func (r *SuiteResult) Passed() bool {
	for _, result := range r.Cases {
		if !result.Passed() {
			return false
		}
	}
	return true
}

// Run reviews every case of the suite, returning what each reported. A case
// which fails its assertions is recorded rather than ending the run, so one run
// reports everything which is wrong.
func (s *Suite) Run(ctx context.Context, opts ...Option) (*SuiteResult, error) {
	result := &SuiteResult{}
	for _, test := range s.Tests {
		if test.Skip {
			for _, c := range test.Cases {
				result.Cases = append(result.Cases, &CaseResult{Test: test.Name, Case: c.Name, Skipped: true})
			}
			continue
		}
		if err := s.runTest(ctx, test, result, opts...); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (s *Suite) runTest(ctx context.Context, test *SuiteTest, result *SuiteResult, opts ...Option) error {
	if test.Template == "" {
		return fmt.Errorf("test %q names no template", test.Name)
	}
	constraint, err := s.readConstraint(test)
	if err != nil {
		return err
	}
	for _, c := range test.Cases {
		caseResult := &CaseResult{Test: test.Name, Case: c.Name, Skipped: c.Skip}
		result.Cases = append(result.Cases, caseResult)
		if c.Skip {
			continue
		}
		// The template is compiled per case so that the inventory of the case
		// backs the lookups the policy makes.
		inventory, err := s.readInventory(c)
		if err != nil {
			caseResult.Failures = append(caseResult.Failures, err)
			continue
		}
		caseOpts := opts
		if len(inventory) != 0 {
			caseOpts = append(append([]Option{}, opts...),
				WithDataProvider(&StaticProvider{Objects: inventory}))
		}
		tmpl, err := CompileFile(filepath.Join(s.dir, test.Template), caseOpts...)
		if err != nil {
			caseResult.Failures = append(caseResult.Failures, err)
			continue
		}
		review, err := s.readReview(c)
		if err != nil {
			caseResult.Failures = append(caseResult.Failures, err)
			continue
		}
		review.Constraint = constraint
		violations, err := tmpl.Review(ctx, *review)
		if err != nil {
			caseResult.Failures = append(caseResult.Failures, err)
			continue
		}
		caseResult.Violations = messagesOf(violations)
		caseResult.Failures = checkAssertions(c.Assertions, caseResult.Violations)
	}
	return nil
}

// checkAssertions reports the assertions the violations of a review do not
// satisfy. A case with no assertion expects no violation, as gator does.
func checkAssertions(assertions []*Assertion, violations []string) []error {
	if len(assertions) == 0 {
		assertions = []*Assertion{{Violations: &intOrString{value: "no"}}}
	}
	var failures []error
	for _, assertion := range assertions {
		matched := violations
		if assertion.Message != "" {
			matched = nil
			for _, violation := range violations {
				if strings.Contains(violation, assertion.Message) {
					matched = append(matched, violation)
				}
			}
		}
		if assertion.Violations.matches(len(matched)) {
			continue
		}
		describe := fmt.Sprintf("wanted %s violations", assertion.Violations)
		if assertion.Violations.String() == "no" {
			describe = "wanted no violations"
		}
		if assertion.Message != "" {
			describe += fmt.Sprintf(" containing %q", assertion.Message)
		}
		failures = append(failures, fmt.Errorf("%s, got %d: %s", describe, len(matched), describeViolations(violations)))
	}
	return failures
}

func describeViolations(violations []string) string {
	if len(violations) == 0 {
		return "the object was admitted"
	}
	quoted := make([]string, 0, len(violations))
	for _, violation := range violations {
		quoted = append(quoted, strconv.Quote(violation))
	}
	return strings.Join(quoted, ", ")
}

func messagesOf(violations []Violation) []string {
	out := make([]string, 0, len(violations))
	for _, violation := range violations {
		out = append(out, violation.Message)
	}
	return out
}

// readConstraint reads the constraint which instantiates the template under
// test, and which carries the parameters its policy reads.
func (s *Suite) readConstraint(test *SuiteTest) (any, error) {
	if test.Constraint == "" {
		return nil, nil
	}
	objects, err := ReadObjects(filepath.Join(s.dir, test.Constraint))
	if err != nil {
		return nil, err
	}
	if len(objects) != 1 {
		return nil, fmt.Errorf("%s: holds %d objects, wanted one constraint", test.Constraint, len(objects))
	}
	return objects[0], nil
}

// readInventory reads the objects which make up the cluster a case is reviewed
// against.
func (s *Suite) readInventory(c *SuiteCase) ([]any, error) {
	var inventory []any
	for _, path := range c.Inventory {
		objects, err := ReadObjects(filepath.Join(s.dir, path))
		if err != nil {
			return nil, err
		}
		inventory = append(inventory, objects...)
	}
	return inventory, nil
}

// readReview reads the object a case is reviewed against.
func (s *Suite) readReview(c *SuiteCase) (*Review, error) {
	if c.Object == "" {
		return nil, fmt.Errorf("case %q names no object", c.Name)
	}
	return ReadReview(filepath.Join(s.dir, c.Object))
}

// ReadReview reads the object under review from a file. A file which holds an
// AdmissionReview supplies the old object and the request attributes as well,
// so that a review recorded from a cluster can be replayed as it happened.
func ReadReview(path string) (*Review, error) {
	objects, err := ReadObjects(path)
	if err != nil {
		return nil, err
	}
	if len(objects) != 1 {
		return nil, fmt.Errorf("%s: holds %d objects, wanted one", path, len(objects))
	}
	object, _ := toStringMap(objects[0])
	if kind, _ := object["kind"].(string); kind != "AdmissionReview" {
		return &Review{Object: objects[0]}, nil
	}
	request, ok := toStringMap(object["request"])
	if !ok {
		return nil, fmt.Errorf("%s: AdmissionReview has no request", path)
	}
	return &Review{
		Object:          request["object"],
		OldObject:       request["oldObject"],
		NamespaceObject: request["namespaceObject"],
		Request:         request,
	}, nil
}

// ReadObjects reads the Kubernetes objects held in a YAML file, which may hold
// more than one document.
func ReadObjects(path string) ([]any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var objects []any
	decoder := yaml.NewDecoder(file)
	for {
		var object any
		err := decoder.Decode(&object)
		if errors.Is(err, io.EOF) {
			return objects, nil
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		if object == nil {
			continue
		}
		objects = append(objects, object)
	}
}
