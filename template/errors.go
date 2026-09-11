// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package template

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/cel-go/cel"
)

// Error describes a single problem with a template, located in the template source rather than
// in the extracted CEL expression, so that positions line up with what the author wrote.
type Error struct {
	// Source is the name the template was compiled under.
	Source string
	// Line and Column are 1-based positions within Source.
	Line   int
	Column int
	// Message describes the problem.
	Message string
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s:%d:%d: %s", e.Source, e.Line, e.Column, e.Message)
}

// Errors is a collection of template errors reported together.
type Errors struct {
	errs []*Error
}

// Error implements the error interface, reporting every issue found, one per line.
func (e *Errors) Error() string {
	msgs := make([]string, 0, len(e.errs))
	for _, err := range e.errs {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "\n")
}

// List returns the individual errors in source order.
func (e *Errors) List() []*Error {
	return e.errs
}

// report adds an error at the given template location.
func (e *Errors) report(source string, loc location, format string, args ...any) {
	e.errs = append(e.errs, &Error{
		Source:  source,
		Line:    loc.line,
		Column:  loc.col,
		Message: fmt.Sprintf(format, args...),
	})
}

// reportIssues translates CEL compile issues, whose positions are relative to the extracted
// expression text, into template-relative errors.
func (e *Errors) reportIssues(source string, loc location, issues *cel.Issues) {
	for _, iss := range issues.Errors() {
		e.errs = append(e.errs, &Error{
			Source:  source,
			Line:    loc.line + iss.Location.Line() - 1,
			Column:  exprColumn(loc, iss),
			Message: iss.Message,
		})
	}
}

// exprColumn maps a column within an expression onto a column within the template. Only the
// first line of a multi-line expression is offset by the action's own indentation.
func exprColumn(loc location, iss *cel.Error) int {
	if iss.Location.Line() == 1 {
		return loc.col + iss.Location.Column()
	}
	return iss.Location.Column() + 1
}

// empty reports whether any errors were collected.
func (e *Errors) empty() bool {
	return len(e.errs) == 0
}

// sorted orders the errors by position so that multi-source compilation is deterministic.
func (e *Errors) sorted() *Errors {
	sort.SliceStable(e.errs, func(i, j int) bool {
		a, b := e.errs[i], e.errs[j]
		if a.Source != b.Source {
			return a.Source < b.Source
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Column < b.Column
	})
	return e
}

// newSyntaxError creates a single-error result for a scanning or parsing failure.
func newSyntaxError(source string, loc location, format string, args ...any) error {
	errs := &Errors{}
	errs.report(source, loc, format, args...)
	return errs
}

// errIn formats a context-free error which a caller attaches a location to.
func errIn(format string, args ...any) error {
	return fmt.Errorf(format, args...)
}

// RenderError describes a failure encountered while rendering a template.
type RenderError struct {
	// Source is the name of the template being rendered.
	Source string
	// Line and Column locate the action which failed.
	Line   int
	Column int
	// Message describes the failure.
	Message string
	// Err is the underlying cause, if any.
	Err error
}

func (e *RenderError) Error() string {
	return fmt.Sprintf("%s:%d:%d: %s", e.Source, e.Line, e.Column, e.Message)
}

// Unwrap exposes the underlying cause for errors.Is and errors.As.
func (e *RenderError) Unwrap() error {
	return e.Err
}

func renderErrorf(source string, loc location, err error, format string, args ...any) error {
	return &RenderError{
		Source:  source,
		Line:    loc.line,
		Column:  loc.col,
		Message: fmt.Sprintf(format, args...),
		Err:     err,
	}
}
