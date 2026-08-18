// Copyright 2026 Google LLC
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

package cost

import (
	"strings"

	"github.com/google/cel-go/common/types"
)

// Hint declares something the estimator could not otherwise know about the size of an input.
type Hint func(*Hints)

// SizeHint bounds the size of the value reached by a field path, e.g. `user.emails` or
// `user.emails.@items`.
//
// The path is the dotted form of [Node.Path]: a variable name followed by field names, with
// '@items', '@keys', or '@values' naming the contents of a list or map.
func SizeHint(path string, maxSize uint64) Hint {
	return func(h *Hints) { h.paths[path] = Ranged(0, maxSize) }
}

// ExactSizeHint fixes the size of the value reached by a field path.
func ExactSizeHint(path string, size uint64) Hint {
	return func(h *Hints) { h.paths[path] = Fixed(size) }
}

// TypeSizeHint bounds the size of every value of a given type, which is the right granularity
// for types whose width is a property of the schema rather than of any one field.
func TypeSizeHint(t *types.Type, maxSize uint64) Hint {
	return func(h *Hints) { h.types[t.String()] = Ranged(0, maxSize) }
}

// NewHints returns an Estimator which answers size questions from declared hints.
//
// Hints cover the common case where an application knows how large its inputs are but has no
// reason to implement a full [Estimator]:
//
//	cost.NewHints(cost.SizeHint("user.emails", 10), cost.TypeSizeHint(types.StringType, 256))
func NewHints(hints ...Hint) *Hints {
	h := &Hints{paths: map[string]Estimate{}, types: map[string]Estimate{}}
	for _, hint := range hints {
		hint(h)
	}
	return h
}

// Hints is an Estimator backed by a set of declared size hints.
type Hints struct {
	paths map[string]Estimate
	types map[string]Estimate
}

// EstimateSize implements the Estimator interface, resolving the most specific hint available
// for the node: first its field path, then its type.
func (h *Hints) EstimateSize(node Node) *Estimate {
	if path := node.Path(); len(path) != 0 {
		if size, found := h.paths[strings.Join(path, ".")]; found {
			return &size
		}
	}
	if t := node.Type(); t != nil {
		if size, found := h.types[t.String()]; found {
			return &size
		}
	}
	return nil
}

// EstimateCall implements the Estimator interface, deferring to the standard cost of the call.
func (h *Hints) EstimateCall(function, overloadID string, operands []Node) *CallEstimate {
	return nil
}
