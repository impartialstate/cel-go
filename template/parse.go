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
	"strings"
	"unicode"
	"unicode/utf8"
)

// location identifies a 1-based line and column within a template source.
type location struct {
	line int
	col  int
}

func (l location) String() string {
	return fmt.Sprintf("%d:%d", l.line, l.col)
}

// tokenKind enumerates the lexical units produced by the scanner.
type tokenKind int

const (
	// tokenText is a literal run of template text which is copied to the output verbatim.
	tokenText tokenKind = iota
	// tokenAction is the body of a `{{ ... }}` pair with delimiters and trim markers removed.
	tokenAction
	// tokenComment is a `{{# ... }}` comment, retained only for whitespace handling.
	tokenComment
)

type token struct {
	kind tokenKind
	// text is either literal text, or the whitespace-trimmed body of an action.
	text string
	// loc points at the first character of literal text, or at the first character of an
	// action body once leading whitespace has been skipped.
	loc location
	// trimLeft and trimRight record the `{{-` and `-}}` markers.
	trimLeft  bool
	trimRight bool
	// keyword is the leading statement keyword of an action, or "" for interpolations.
	keyword string
}

const (
	leftDelim  = "{{"
	rightDelim = "}}"
)

// keywords are the action-leading words which introduce a statement rather than an
// interpolated expression. Every entry other than "define" is also a CEL reserved word, so
// recognizing them here cannot shadow an otherwise valid expression.
var keywords = map[string]bool{
	"if":       true,
	"else":     true,
	"end":      true,
	"for":      true,
	"let":      true,
	"break":    true,
	"continue": true,
	"define":   true,
}

// scanner splits template source into text and action tokens.
type scanner struct {
	name string
	src  string
	pos  int
	line int
	col  int
}

// scan splits src into an alternating sequence of text and action tokens. Text tokens may be
// empty; they are retained between adjacent actions so that whitespace trimming can be applied
// uniformly, and are dropped once trimming is complete.
func scan(name, src string) ([]*token, error) {
	s := &scanner{name: name, src: src, line: 1, col: 1}
	var toks []*token
	for {
		start := s.pos
		startLoc := s.loc()
		idx := strings.Index(s.src[s.pos:], leftDelim)
		if idx < 0 {
			toks = append(toks, &token{kind: tokenText, text: s.src[start:], loc: startLoc})
			return toks, nil
		}
		s.advance(idx)
		toks = append(toks, &token{kind: tokenText, text: s.src[start:s.pos], loc: startLoc})
		tok, err := s.scanAction()
		if err != nil {
			return nil, err
		}
		toks = append(toks, tok)
	}
}

// scanAction consumes a `{{ ... }}` pair positioned at the opening delimiter.
func (s *scanner) scanAction() (*token, error) {
	openLoc := s.loc()
	s.advance(len(leftDelim))
	tok := &token{kind: tokenAction}
	if byteAt(s.src, s.pos) == '-' && isSpace(byteAt(s.src, s.pos+1)) {
		// A '-' only trims when followed by whitespace, so `{{-x}}` remains a negation.
		tok.trimLeft = true
		s.advance(1)
	}
	bodyLoc := s.loc()
	if byteAt(s.src, s.pos) == '#' {
		end := strings.Index(s.src[s.pos:], rightDelim)
		if end < 0 {
			return nil, newSyntaxError(s.name, openLoc, "unclosed comment: missing %q", rightDelim)
		}
		body := s.src[s.pos : s.pos+end]
		s.advance(end + len(rightDelim))
		tok.kind = tokenComment
		tok.loc = bodyLoc
		tok.trimRight = strings.HasSuffix(strings.TrimRight(body, " \t"), "-")
		return tok, nil
	}
	end, err := s.findActionEnd(openLoc)
	if err != nil {
		return nil, err
	}
	body := s.src[s.pos:end]
	if strings.HasSuffix(body, "-") && isSpace(byteAt(body, len(body)-2)) {
		tok.trimRight = true
		body = body[:len(body)-1]
	}
	s.advance(end + len(rightDelim) - s.pos)
	tok.text, tok.loc = trimActionBody(body, bodyLoc)
	if tok.text == "" {
		return nil, newSyntaxError(s.name, openLoc, "empty action")
	}
	if word := leadingWord(tok.text); keywords[word] {
		tok.keyword = word
	}
	return tok, nil
}

// findActionEnd locates the closing delimiter for the action body starting at s.pos, skipping
// over CEL string literals so that `{{ "}}" }}` scans correctly.
func (s *scanner) findActionEnd(openLoc location) (int, error) {
	i := s.pos
	for i < len(s.src) {
		switch s.src[i] {
		case '"', '\'':
			next, err := skipStringLiteral(s.src, i)
			if err != nil {
				return 0, newSyntaxError(s.name, s.locAt(i), "%s", err.Error())
			}
			i = next
		case '}':
			if strings.HasPrefix(s.src[i:], rightDelim) {
				return i, nil
			}
			i++
		default:
			i++
		}
	}
	return 0, newSyntaxError(s.name, openLoc, "unclosed action: missing %q", rightDelim)
}

// skipStringLiteral returns the index just past the CEL string literal beginning at i.
func skipStringLiteral(src string, i int) (int, error) {
	quote := src[i]
	// Raw strings suppress backslash escaping; they are introduced by an r/R prefix which may
	// itself be preceded by a bytes marker, as in `rb"\d+"`.
	raw := false
	if i > 0 && (src[i-1] == 'r' || src[i-1] == 'R') {
		raw = true
	}
	tripleQuote := strings.Repeat(string(quote), 3)
	if strings.HasPrefix(src[i:], tripleQuote) {
		end := strings.Index(src[i+3:], tripleQuote)
		if end < 0 {
			return 0, fmt.Errorf("unterminated string literal")
		}
		return i + 3 + end + 3, nil
	}
	i++
	for i < len(src) {
		switch src[i] {
		case '\\':
			if raw {
				i++
				continue
			}
			i += 2
		case quote:
			return i + 1, nil
		case '\n':
			return 0, fmt.Errorf("unterminated string literal")
		default:
			i++
		}
	}
	return 0, fmt.Errorf("unterminated string literal")
}

// advance moves the scanner forward n bytes, maintaining the line and column counters.
func (s *scanner) advance(n int) {
	for i := 0; i < n && s.pos < len(s.src); i++ {
		if s.src[s.pos] == '\n' {
			s.line++
			s.col = 1
		} else if utf8.RuneStart(s.src[s.pos]) {
			s.col++
		}
		s.pos++
	}
}

func (s *scanner) loc() location {
	return location{line: s.line, col: s.col}
}

// locAt computes the location of an absolute byte offset at or after the current position.
func (s *scanner) locAt(offset int) location {
	l := s.loc()
	for i := s.pos; i < offset && i < len(s.src); i++ {
		if s.src[i] == '\n' {
			l.line++
			l.col = 1
		} else if utf8.RuneStart(s.src[i]) {
			l.col++
		}
	}
	return l
}

// trimActionBody strips surrounding whitespace from an action body and reports the location of
// the first remaining character.
func trimActionBody(body string, loc location) (string, location) {
	i := 0
	for i < len(body) && isSpace(body[i]) {
		if body[i] == '\n' {
			loc.line++
			loc.col = 1
		} else {
			loc.col++
		}
		i++
	}
	return strings.TrimRight(body[i:], " \t\r\n"), loc
}

// applyTrim rewrites text tokens according to the explicit `{{-` and `-}}` markers and, when
// standalone trimming is enabled, according to the standalone-line rule: a line containing
// nothing but a single control action is removed from the output entirely.
func applyTrim(toks []*token, standalone bool) []*token {
	for i, tok := range toks {
		if tok.kind == tokenText {
			continue
		}
		if tok.trimLeft && i > 0 {
			toks[i-1].text = strings.TrimRight(toks[i-1].text, " \t\r\n")
		}
		if tok.trimRight && i+1 < len(toks) {
			toks[i+1].text = strings.TrimLeft(toks[i+1].text, " \t\r\n")
		}
	}
	if standalone {
		trimStandaloneLines(toks)
	}
	out := make([]*token, 0, len(toks))
	for _, tok := range toks {
		if tok.kind == tokenComment || (tok.kind == tokenText && tok.text == "") {
			continue
		}
		out = append(out, tok)
	}
	return out
}

// trimStandaloneLines walks the token stream tracking whether the current output line is still
// blank, and removes any control action which is the only content on its line along with the
// newline that follows it. The scanner always emits a text token between actions, so an
// action's neighbours are guaranteed to be text.
func trimStandaloneLines(toks []*token) {
	blankLine := true
	for i, tok := range toks {
		switch {
		case tok.kind == tokenText:
			if nl := strings.LastIndexByte(tok.text, '\n'); nl >= 0 {
				blankLine = isBlank(tok.text[nl+1:])
			} else if tok.text != "" {
				blankLine = blankLine && isBlank(tok.text)
			}
		case tok.kind == tokenAction && tok.keyword == "":
			blankLine = false
		default:
			if blankLine && removeStandalone(toks, i) {
				blankLine = true
			} else {
				blankLine = false
			}
		}
	}
}

// removeStandalone strips the indentation preceding a control action and the newline which
// follows it, reporting whether the action was in fact alone on its line.
func removeStandalone(toks []*token, i int) bool {
	next := toks[i+1]
	rest := ""
	if idx := strings.IndexByte(next.text, '\n'); idx >= 0 {
		if !isBlank(next.text[:idx]) {
			return false
		}
		rest = next.text[idx+1:]
	} else if i+1 != len(toks)-1 || !isBlank(next.text) {
		// Without a newline the line continues past next, which is only blank at end of source.
		return false
	}
	prev := toks[i-1]
	prev.text = strings.TrimRight(prev.text, " \t")
	next.text = rest
	return true
}

func isBlank(s string) bool {
	return strings.TrimLeft(s, " \t\r") == ""
}

func leadingWord(s string) string {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') {
			continue
		}
		return s[:i]
	}
	return s
}

func isSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}

func byteAt(s string, i int) byte {
	if i < 0 || i >= len(s) {
		return 0
	}
	return s[i]
}

func isIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		if r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r)) {
			continue
		}
		return false
	}
	return true
}
