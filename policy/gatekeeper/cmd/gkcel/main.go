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

// gkcel compiles and tests the CEL within OPA Gatekeeper ConstraintTemplates,
// without a cluster and without Gatekeeper installed.
//
//	gkcel check policies/**/template.yaml
//	gkcel test  policies/**/suite.yaml
//	gkcel review -template template.yaml -constraint constraint.yaml -object pod.yaml
//
// check reports the mistakes in a template's expressions against the line and
// column which contains them. test runs a gator test suite against the CEL of
// the templates it names. review evaluates one object and prints the violations
// a cluster would report.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/cel-go/policy/gatekeeper"
)

const usage = `gkcel compiles and tests the CEL within Gatekeeper ConstraintTemplates.

Usage:
  gkcel check [flags] <template.yaml>...    compile the templates and report mistakes
  gkcel test <suite.yaml>...                run gator test suites against the templates
  gkcel review [flags]                      review one object against a template

Check flags:
  -crd          a CustomResourceDefinition whose schema types the object under review
  -crd-version  the version of the definition to read; defaults to the stored version

Review flags:
  -template     the ConstraintTemplate to review against (required)
  -constraint   the constraint which supplies the policy's parameters
  -object       the object under review, or an AdmissionReview (required)
  -old-object   the object as it exists in the cluster, for an update or delete
  -namespace-object the namespace of the object under review
  -inventory    a file of objects the policy may read; repeatable
  -crd          a CustomResourceDefinition whose schema types the object under review
  -crd-version  the version of the definition to read; defaults to the stored version
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	var err error
	var failed bool
	switch os.Args[1] {
	case "check":
		failed, err = check(os.Args[2:])
	case "test":
		failed, err = test(os.Args[2:])
	case "review":
		failed, err = review(os.Args[2:])
	case "help", "-h", "-help", "--help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "gkcel: %v\n", err)
		os.Exit(2)
	}
	if failed {
		os.Exit(1)
	}
}

// check compiles each template and reports the mistakes within it.
func check(args []string) (bool, error) {
	flags := flag.NewFlagSet("check", flag.ContinueOnError)
	crdPath := flags.String("crd", "", "a CustomResourceDefinition whose schema types the object under review")
	crdVersion := flags.String("crd-version", "", "the version of the definition to read; defaults to the stored version")
	if err := flags.Parse(args); err != nil {
		return false, err
	}
	paths := flags.Args()
	if len(paths) == 0 {
		return false, fmt.Errorf("check needs at least one template")
	}
	opts, err := schemaOptions(*crdPath, *crdVersion)
	if err != nil {
		return false, err
	}
	failed := false
	for _, path := range expand(paths) {
		tmpl, err := gatekeeper.CompileFile(path, opts...)
		if err != nil {
			failed = true
			fmt.Printf("FAIL %s\n%s\n", path, indent(err.Error()))
			continue
		}
		fmt.Printf("OK   %s (%s)\n", path, tmpl.Name())
	}
	return failed, nil
}

// test runs each suite and reports the cases which did not meet their
// assertions.
func test(paths []string) (bool, error) {
	if len(paths) == 0 {
		return false, fmt.Errorf("test needs at least one suite")
	}
	failed := false
	for _, path := range expand(paths) {
		suite, err := gatekeeper.ReadSuite(path)
		if err != nil {
			return false, err
		}
		result, err := suite.Run(context.Background())
		if err != nil {
			return false, err
		}
		fmt.Printf("%s\n%s", path, indent(strings.TrimRight(result.Report(), "\n")))
		fmt.Println()
		failed = failed || !result.Passed()
	}
	return failed, nil
}

// review evaluates one object and prints the violations a cluster would report.
func review(args []string) (bool, error) {
	flags := flag.NewFlagSet("review", flag.ContinueOnError)
	templatePath := flags.String("template", "", "the ConstraintTemplate to review against")
	constraintPath := flags.String("constraint", "", "the constraint which supplies the policy's parameters")
	objectPath := flags.String("object", "", "the object under review, or an AdmissionReview")
	oldObjectPath := flags.String("old-object", "", "the object as it exists in the cluster")
	namespacePath := flags.String("namespace-object", "", "the namespace of the object under review")
	crdPath := flags.String("crd", "", "a CustomResourceDefinition whose schema types the object under review")
	crdVersion := flags.String("crd-version", "", "the version of the definition to read; defaults to the stored version")
	var inventoryPaths pathList
	flags.Var(&inventoryPaths, "inventory", "a file of objects the policy may read; repeatable")
	if err := flags.Parse(args); err != nil {
		return false, err
	}
	if *templatePath == "" || *objectPath == "" {
		return false, fmt.Errorf("review needs -template and -object")
	}
	var inventory []any
	for _, path := range inventoryPaths {
		objects, err := gatekeeper.ReadObjects(path)
		if err != nil {
			return false, err
		}
		inventory = append(inventory, objects...)
	}
	opts, err := schemaOptions(*crdPath, *crdVersion)
	if err != nil {
		return false, err
	}
	opts = append(opts, gatekeeper.WithDataProvider(&gatekeeper.StaticProvider{Objects: inventory}))
	tmpl, err := gatekeeper.CompileFile(*templatePath, opts...)
	if err != nil {
		return false, err
	}
	request, err := gatekeeper.ReadReview(*objectPath)
	if err != nil {
		return false, err
	}
	for _, input := range []struct {
		path  string
		field *any
	}{
		{*oldObjectPath, &request.OldObject},
		{*namespacePath, &request.NamespaceObject},
		{*constraintPath, &request.Constraint},
	} {
		if input.path == "" {
			continue
		}
		object, err := readSingleObject(input.path)
		if err != nil {
			return false, err
		}
		*input.field = object
	}
	violations, err := tmpl.Review(context.Background(), *request)
	if err != nil {
		return false, err
	}
	if len(violations) == 0 {
		fmt.Printf("admitted by %s\n", tmpl.Name())
		return false, nil
	}
	fmt.Printf("%d violation(s) of %s\n", len(violations), tmpl.Name())
	for _, violation := range violations {
		fmt.Printf("  - %s\n", violation.Message)
	}
	return true, nil
}

// schemaOptions reads the definition of the resource under review, so that the
// fields a policy reads from it are checked against its schema.
func schemaOptions(crdPath, version string) ([]gatekeeper.Option, error) {
	if crdPath == "" {
		return nil, nil
	}
	schema, err := gatekeeper.ReadCRDSchema(crdPath, version)
	if err != nil {
		return nil, err
	}
	return []gatekeeper.Option{gatekeeper.WithObjectSchema(schema)}, nil
}

func readSingleObject(path string) (any, error) {
	objects, err := gatekeeper.ReadObjects(path)
	if err != nil {
		return nil, err
	}
	if len(objects) != 1 {
		return nil, fmt.Errorf("%s: holds %d objects, wanted one", path, len(objects))
	}
	return objects[0], nil
}

// pathList collects a flag which may be given more than once.
type pathList []string

func (l *pathList) String() string {
	return strings.Join(*l, ", ")
}

func (l *pathList) Set(value string) error {
	*l = append(*l, value)
	return nil
}

// expand resolves the glob patterns a shell did not, so that the same command
// works whether or not the shell expands `**`.
func expand(paths []string) []string {
	var out []string
	for _, path := range paths {
		if !strings.ContainsAny(path, "*?[") {
			out = append(out, path)
			continue
		}
		matches, err := filepath.Glob(path)
		if err != nil || len(matches) == 0 {
			out = append(out, path)
			continue
		}
		out = append(out, matches...)
	}
	return out
}

func indent(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = "  " + line
	}
	return strings.Join(lines, "\n") + "\n"
}
