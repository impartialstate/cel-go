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
	"io"
	"sort"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
)

// defaultMaxOutput bounds the output of a single render unless the caller configures its own
// limit. It is generous for documents and configuration files while still preventing a
// runaway expansion from exhausting memory.
const defaultMaxOutput = 8 << 20

// config holds the settings shared by a compiler, its templates, and every render.
type config struct {
	varOpts    []cel.EnvOption
	envOpts    []cel.EnvOption
	progOpts   []cel.ProgramOption
	escaper    Escaper
	maxOutput  int
	standalone bool
	stdlib     bool
}

// Option configures a Compiler.
type Option func(*config) error

// Variable declares an input variable available to the top-level body of every template in the
// set. Sub-templates declared with {{ define }} do not see these variables; they receive their
// inputs as declared parameters.
func Variable(name string, t *cel.Type) Option {
	return func(c *config) error {
		c.varOpts = append(c.varOpts, cel.Variable(name, t))
		return nil
	}
}

// CELOptions adds environment options to the CEL environment templates are checked against,
// for registering custom functions, protobuf types, or extension libraries.
//
// Variables declared this way are visible to sub-templates as well, which is usually not what
// you want; prefer Variable for template inputs.
func CELOptions(opts ...cel.EnvOption) Option {
	return func(c *config) error {
		c.envOpts = append(c.envOpts, opts...)
		return nil
	}
}

// ProgramOptions adds CEL program options applied to every expression in the template,
// such as cel.EvalOptions or cel.InterruptCheckFrequency.
func ProgramOptions(opts ...cel.ProgramOption) Option {
	return func(c *config) error {
		c.progOpts = append(c.progOpts, opts...)
		return nil
	}
}

// CostLimit bounds the evaluation cost of each expression in the template, aborting evaluation
// when the limit is exceeded.
func CostLimit(limit uint64) Option {
	return ProgramOptions(cel.CostLimit(limit))
}

// Escape applies an escaper to every interpolated value. Values wrapped in raw() and the
// results of sub-templates are written through unchanged, since they are already escaped.
func Escape(e Escaper) Option {
	return func(c *config) error {
		c.escaper = e
		return nil
	}
}

// MaxOutputBytes limits the size of a single rendered document. A value of zero disables the
// limit. Rendering past the limit fails with ErrOutputLimit.
func MaxOutputBytes(n int) Option {
	return func(c *config) error {
		if n < 0 {
			return fmt.Errorf("template: MaxOutputBytes must not be negative: %d", n)
		}
		c.maxOutput = n
		return nil
	}
}

// StandaloneLines controls whether a line containing nothing but a single control action, such
// as {{ if }} or {{ end }}, is removed from the output along with its newline.
//
// It is enabled by default. Disabling it restores the behavior of Go's text/template, where
// every newline a control action sits on is emitted unless trimmed with {{- and -}}.
func StandaloneLines(enabled bool) Option {
	return func(c *config) error {
		c.standalone = enabled
		return nil
	}
}

// StdLib controls whether the template function library and the CEL extension libraries it
// builds on are registered. It is enabled by default.
func StdLib(enabled bool) Option {
	return func(c *config) error {
		c.stdlib = enabled
		return nil
	}
}

// Compiler parses and type-checks a set of templates which may refer to one another.
type Compiler struct {
	cfg     *config
	baseEnv *cel.Env
	sources []*parsedSource
	seen    map[string]bool
}

// NewCompiler creates a template compiler configured with the given options.
func NewCompiler(opts ...Option) (*Compiler, error) {
	cfg := &config{maxOutput: defaultMaxOutput, standalone: true, stdlib: true}
	for _, opt := range opts {
		if err := opt(cfg); err != nil {
			return nil, err
		}
	}
	envOpts := make([]cel.EnvOption, 0, len(cfg.envOpts)+8)
	if cfg.stdlib {
		envOpts = append(envOpts, stdLibOptions()...)
	}
	envOpts = append(envOpts, cfg.envOpts...)
	baseEnv, err := cel.NewEnv(envOpts...)
	if err != nil {
		return nil, fmt.Errorf("template: invalid environment: %w", err)
	}
	return &Compiler{cfg: cfg, baseEnv: baseEnv, seen: map[string]bool{}}, nil
}

// stdLibOptions is the environment a template is checked against by default. The CEL extension
// libraries cover most of what a template author reaches for, and the template library adds
// the output-shaping functions which only make sense when producing text.
func stdLibOptions() []cel.EnvOption {
	return []cel.EnvOption{
		cel.OptionalTypes(),
		ext.Strings(),
		ext.Lists(),
		ext.Sets(),
		ext.Math(),
		ext.Encoders(),
		ext.Regex(),
		ext.Bindings(),
		ext.TwoVarComprehensions(),
		TemplateFuncs(),
	}
}

// AddSource parses a template under the given name and adds it to the set. Templates defined
// with {{ define }} in any source are visible to every other source in the set.
func (c *Compiler) AddSource(name, src string) error {
	if c.seen[name] {
		return fmt.Errorf("template: %q was already added to this compiler", name)
	}
	ps, err := parse(name, src, c.cfg.standalone)
	if err != nil {
		return err
	}
	c.seen[name] = true
	c.sources = append(c.sources, ps)
	return nil
}

// Compile type-checks every source added to the compiler and returns the renderable set.
//
// All errors found across all sources are reported together, ordered by source position.
func (c *Compiler) Compile() (*Set, error) {
	return compileSet(c.cfg, c.baseEnv, c.sources)
}

// Set is a compiled group of templates which share a namespace of {{ define }} sub-templates.
type Set struct {
	cfg       *config
	env       *cel.Env
	templates map[string]*Template
	names     []string
}

// Names lists the templates in the set in the order they were added.
func (s *Set) Names() []string {
	out := append([]string{}, s.names...)
	return out
}

// Template returns the named template, reporting whether it was found.
func (s *Set) Template(name string) (*Template, bool) {
	t, found := s.templates[name]
	return t, found
}

// Render writes the named template to w.
func (s *Set) Render(w io.Writer, name string, vars any) error {
	t, found := s.templates[name]
	if !found {
		return fmt.Errorf("template: no template named %q; the set contains %v", name, sortedNames(s.names))
	}
	return t.Render(w, vars)
}

// Env returns the CEL environment the top-level template bodies were checked against. It
// includes every sub-template as a function, which makes it useful for tooling and for
// documenting the surface available to template authors.
func (s *Set) Env() *cel.Env {
	return s.env
}

func sortedNames(names []string) []string {
	out := append([]string{}, names...)
	sort.Strings(out)
	return out
}

// Template is a single compiled template, ready to render.
type Template struct {
	name   string
	source string
	body   []node
	params []param
	cfg    *config
}

// Name returns the name the template was compiled under.
func (t *Template) Name() string {
	return t.name
}

// Compile parses and type-checks a single template source.
func Compile(name, src string, opts ...Option) (*Template, error) {
	c, err := NewCompiler(opts...)
	if err != nil {
		return nil, err
	}
	if err := c.AddSource(name, src); err != nil {
		return nil, err
	}
	set, err := c.Compile()
	if err != nil {
		return nil, err
	}
	t, _ := set.Template(name)
	return t, nil
}

// MustCompile is like Compile but panics on failure. It is intended for templates which are
// compiled at process start from constant source.
func MustCompile(name, src string, opts ...Option) *Template {
	t, err := Compile(name, src, opts...)
	if err != nil {
		panic(err)
	}
	return t
}
