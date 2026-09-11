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
	"strings"
	"unicode/utf8"
)

// parsedSource is the result of parsing one template source: a body plus the defines it
// contributes to the enclosing template set.
type parsedSource struct {
	name    string
	body    []node
	defines []*define
}

type parser struct {
	name    string
	toks    []*token
	pos     int
	depth   int
	defines []*define
}

// parse converts template source into a body and the set of defines it declares.
func parse(name, src string, standaloneTrim bool) (*parsedSource, error) {
	toks, err := scan(name, src)
	if err != nil {
		return nil, err
	}
	p := &parser{name: name, toks: applyTrim(toks, standaloneTrim)}
	ps := &parsedSource{name: name}
	body, term, err := p.parseBody(nil)
	if err != nil {
		return nil, err
	}
	if term != nil {
		return nil, newSyntaxError(name, term.loc, "unexpected %s", describe(term))
	}
	ps.body = body
	ps.defines = p.defines
	return ps, nil
}

// describe renders a token for use in an error message.
func describe(t *token) string {
	if t == nil {
		return "end of template"
	}
	return "{{ " + t.text + " }}"
}

// parseBody consumes nodes until end of input or until a token whose keyword appears in
// terminators. The terminating token is returned without being consumed by the caller's block.
func (p *parser) parseBody(terminators map[string]bool) ([]node, *token, error) {
	if terminators != nil {
		p.depth++
		defer func() { p.depth-- }()
	}
	var body []node
	for {
		tok := p.peek()
		if tok == nil {
			return body, nil, nil
		}
		if tok.kind == tokenAction && terminators[tok.keyword] {
			p.next()
			return body, tok, nil
		}
		n, err := p.parseNode()
		if err != nil {
			return nil, nil, err
		}
		if n != nil {
			body = append(body, n)
		}
	}
}

func (p *parser) parseNode() (node, error) {
	tok := p.next()
	if tok.kind == tokenText {
		return &textNode{loc: tok.loc, text: tok.text}, nil
	}
	switch tok.keyword {
	case "":
		return &interpNode{loc: tok.loc, expr: newExpr(tok)}, nil
	case "if":
		return p.parseIf(tok)
	case "for":
		return p.parseFor(tok)
	case "let":
		return p.parseLet(tok)
	case "define":
		if p.depth != 0 {
			return nil, newSyntaxError(p.name, tok.loc,
				"{{ define }} may only appear at the top level of a template")
		}
		return nil, p.parseDefine(tok)
	case "break", "continue":
		if rest := strings.TrimSpace(strings.TrimPrefix(tok.text, tok.keyword)); rest != "" {
			return nil, newSyntaxError(p.name, tok.loc, "%s takes no arguments", tok.keyword)
		}
		return &jumpNode{loc: tok.loc, isContinue: tok.keyword == "continue"}, nil
	default:
		return nil, newSyntaxError(p.name, tok.loc, "unexpected %s", describe(tok))
	}
}

var ifTerminators = map[string]bool{"else": true, "end": true}

func (p *parser) parseIf(tok *token) (node, error) {
	cond, off, err := p.argument(tok, "if")
	if err != nil {
		return nil, err
	}
	n := &ifNode{loc: tok.loc}
	arm := &condNode{loc: tok.loc, cond: &expression{loc: locAt(tok, off), src: cond}}
	for {
		body, term, err := p.parseBody(ifTerminators)
		if err != nil {
			return nil, err
		}
		arm.body = body
		n.arms = append(n.arms, arm)
		if term == nil {
			return nil, newSyntaxError(p.name, tok.loc, "unclosed {{ if }}: missing {{ end }}")
		}
		if term.keyword == "end" {
			return n, nil
		}
		rest := strings.TrimSpace(strings.TrimPrefix(term.text, "else"))
		if rest == "" {
			alt, endTok, err := p.parseBody(map[string]bool{"end": true})
			if err != nil {
				return nil, err
			}
			if endTok == nil {
				return nil, newSyntaxError(p.name, tok.loc, "unclosed {{ if }}: missing {{ end }}")
			}
			n.alt = alt
			if n.alt == nil {
				n.alt = []node{}
			}
			return n, nil
		}
		if leadingWord(rest) != "if" {
			return nil, newSyntaxError(p.name, term.loc, "expected {{ else }} or {{ else if <expr> }}")
		}
		condSrc := strings.TrimLeft(strings.TrimPrefix(rest, "if"), " \t\r\n")
		if strings.TrimSpace(condSrc) == "" {
			return nil, newSyntaxError(p.name, term.loc, "{{ else if }} requires a condition")
		}
		condOff := len(term.text) - len(condSrc)
		arm = &condNode{
			loc:  term.loc,
			cond: &expression{loc: locAt(term, condOff), src: strings.TrimSpace(condSrc)},
		}
	}
}

var forTerminators = map[string]bool{"else": true, "end": true}

func (p *parser) parseFor(tok *token) (node, error) {
	rest, off, err := p.argument(tok, "for")
	if err != nil {
		return nil, err
	}
	vars, iterSrc, iterOff, err := splitForClause(rest)
	if err != nil {
		return nil, newSyntaxError(p.name, tok.loc, "%s", err.Error())
	}
	n := &forNode{
		loc:  tok.loc,
		vars: vars,
		iter: &expression{loc: locAt(tok, off+iterOff), src: iterSrc},
	}
	body, term, err := p.parseBody(forTerminators)
	if err != nil {
		return nil, err
	}
	if term == nil {
		return nil, newSyntaxError(p.name, tok.loc, "unclosed {{ for }}: missing {{ end }}")
	}
	n.body = body
	if term.keyword == "end" {
		return n, nil
	}
	if strings.TrimSpace(strings.TrimPrefix(term.text, "else")) != "" {
		return nil, newSyntaxError(p.name, term.loc, "{{ else if }} is not valid inside {{ for }}")
	}
	alt, endTok, err := p.parseBody(map[string]bool{"end": true})
	if err != nil {
		return nil, err
	}
	if endTok == nil {
		return nil, newSyntaxError(p.name, tok.loc, "unclosed {{ for }}: missing {{ end }}")
	}
	n.alt = alt
	if n.alt == nil {
		n.alt = []node{}
	}
	return n, nil
}

// splitForClause parses `x in xs` or `k, v in xs` into its variable names, its range
// expression, and the offset of that expression within the clause.
func splitForClause(s string) ([]string, string, int, error) {
	idx := indexKeyword(s, "in")
	if idx < 0 {
		return nil, "", 0, errIn("expected {{ for <var> in <expr> }}")
	}
	names := strings.TrimSpace(s[:idx])
	tail := s[idx+len("in"):]
	iter := strings.TrimLeft(tail, " \t\r\n")
	if strings.TrimSpace(iter) == "" {
		return nil, "", 0, errIn("missing range expression after 'in'")
	}
	iterOff := len(s) - len(iter)
	var vars []string
	for _, part := range strings.Split(names, ",") {
		name := strings.TrimSpace(part)
		if !isIdent(name) {
			return nil, "", 0, errIn("invalid loop variable %q", name)
		}
		vars = append(vars, name)
	}
	if len(vars) > 2 {
		return nil, "", 0, errIn("at most two loop variables may be declared")
	}
	if len(vars) == 2 && vars[0] == vars[1] {
		return nil, "", 0, errIn("duplicate loop variable %q", vars[0])
	}
	return vars, strings.TrimSpace(iter), iterOff, nil
}

// indexKeyword finds the offset of a standalone word in s, ignoring occurrences inside
// identifiers and string literals.
func indexKeyword(s, word string) int {
	for i := 0; i+len(word) <= len(s); i++ {
		switch s[i] {
		case '\'', '"':
			next, err := skipStringLiteral(s, i)
			if err != nil {
				return -1
			}
			i = next - 1
			continue
		}
		if !strings.HasPrefix(s[i:], word) {
			continue
		}
		if i > 0 && isWordByte(s[i-1]) {
			continue
		}
		if i+len(word) < len(s) && isWordByte(s[i+len(word)]) {
			continue
		}
		return i
	}
	return -1
}

func isWordByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func (p *parser) parseLet(tok *token) (node, error) {
	rest, off, err := p.argument(tok, "let")
	if err != nil {
		return nil, err
	}
	eq := strings.Index(rest, "=")
	if eq < 0 {
		return nil, newSyntaxError(p.name, tok.loc, "expected {{ let <name> = <expr> }}")
	}
	name := strings.TrimSpace(rest[:eq])
	if !isIdent(name) {
		return nil, newSyntaxError(p.name, tok.loc, "invalid variable name %q", name)
	}
	tail := strings.TrimLeft(rest[eq+1:], " \t\r\n")
	if strings.TrimSpace(tail) == "" {
		return nil, newSyntaxError(p.name, tok.loc, "missing expression after '=' in {{ let %s }}", name)
	}
	exprLoc := locAt(tok, off+len(rest)-len(tail))
	return &letNode{
		loc:  tok.loc,
		name: name,
		expr: &expression{loc: exprLoc, src: strings.TrimSpace(tail)},
	}, nil
}

func (p *parser) parseDefine(tok *token) error {
	rest, _, err := p.argument(tok, "define")
	if err != nil {
		return err
	}
	open := strings.Index(rest, "(")
	if open < 0 || !strings.HasSuffix(rest, ")") {
		return newSyntaxError(p.name, tok.loc,
			"expected {{ define <name>(<param> <type>, ...) }}")
	}
	name := strings.TrimSpace(rest[:open])
	if !isIdent(name) {
		return newSyntaxError(p.name, tok.loc, "invalid template name %q", name)
	}
	params, err := parseParams(rest[open+1 : len(rest)-1])
	if err != nil {
		return newSyntaxError(p.name, tok.loc, "in {{ define %s }}: %s", name, err.Error())
	}
	for i := range params {
		params[i].loc = tok.loc
	}
	body, term, err := p.parseBody(map[string]bool{"end": true})
	if err != nil {
		return err
	}
	if term == nil {
		return newSyntaxError(p.name, tok.loc, "unclosed {{ define %s }}: missing {{ end }}", name)
	}
	p.defines = append(p.defines, &define{
		loc:    tok.loc,
		source: p.name,
		name:   name,
		params: params,
		body:   body,
	})
	return nil
}

// parseParams parses a comma separated `name type` parameter list. Type arguments may contain
// commas, so splitting tracks bracket depth.
func parseParams(s string) ([]param, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	var params []param
	seen := map[string]bool{}
	for _, decl := range splitTopLevel(s) {
		decl = strings.TrimSpace(decl)
		sp := strings.IndexFunc(decl, func(r rune) bool { return r == ' ' || r == '\t' })
		if sp < 0 {
			return nil, errIn("parameter %q is missing a type", decl)
		}
		name := decl[:sp]
		typeName := strings.TrimSpace(decl[sp+1:])
		if !isIdent(name) {
			return nil, errIn("invalid parameter name %q", name)
		}
		if seen[name] {
			return nil, errIn("duplicate parameter %q", name)
		}
		if typeName == "" {
			return nil, errIn("parameter %q is missing a type", name)
		}
		seen[name] = true
		params = append(params, param{name: name, typeName: typeName})
	}
	return params, nil
}

// splitTopLevel splits on commas which are not nested inside <>, () or [].
func splitTopLevel(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '<', '(', '[':
			depth++
		case '>', ')', ']':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

// argument returns the action text following its leading keyword, along with the offset of
// that text within the action body.
func (p *parser) argument(tok *token, keyword string) (string, int, error) {
	trimmed := strings.TrimPrefix(tok.text, keyword)
	rest := strings.TrimLeft(trimmed, " \t\r\n")
	if strings.TrimSpace(rest) == "" {
		return "", 0, newSyntaxError(p.name, tok.loc, "{{ %s }} requires an argument", keyword)
	}
	return strings.TrimSpace(rest), len(tok.text) - len(rest), nil
}

// locAt reports the location of a byte offset within an action body, so that expression
// diagnostics point at the expression rather than at the action which contains it.
func locAt(tok *token, offset int) location {
	loc := tok.loc
	for i := 0; i < offset && i < len(tok.text); i++ {
		if tok.text[i] == '\n' {
			loc.line++
			loc.col = 1
		} else if utf8.RuneStart(tok.text[i]) {
			loc.col++
		}
	}
	return loc
}

func newExpr(tok *token) *expression {
	return &expression{loc: tok.loc, src: tok.text}
}

func (p *parser) peek() *token {
	if p.pos >= len(p.toks) {
		return nil
	}
	return p.toks[p.pos]
}

func (p *parser) next() *token {
	tok := p.peek()
	if tok != nil {
		p.pos++
	}
	return tok
}
