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
	"github.com/google/cel-go/common/overloads"
)

// StandardModel returns the cost model for a standard library overload.
//
// The models below are the single source of truth for the cost of the standard library. The
// estimator evaluates them over the sizes it derives from the AST and the tracker evaluates them
// over the sizes of the values which were actually passed to the call, so the two halves of the
// cost system cannot drift apart.
//
// Overloads without a model are assumed to be O(1) and cost a single unit:
//   - Conversions, since none perform a traversal of a type of unbound length.
//   - Computing the size of strings, byte sequences, lists and maps.
//   - Logical operations and all operators on fixed width scalars.
//   - List concatenation, which is lazy, though index lookup afterwards is O(c) in the number
//     of concatenated lists.
func StandardModel(overloadID string) (Model, bool) {
	model, found := standardModels[overloadID]
	return model, found
}

var standardModels = map[string]Model{
	// O(n) traversals of a single string or byte sequence.
	overloads.ExtFormatString: {
		Traversed: Operand(0).Scale(StringTraversalCostFactor),
	},
	overloads.StringToBytes: {
		Traversed: Operand(0).Scale(StringTraversalCostFactor),
		// Each unicode code point converts to at most 4 bytes.
		Result: Between(Operand(0), Operand(0).Times(Const(4))),
	},
	overloads.BytesToString: {
		Traversed: Operand(0).Scale(StringTraversalCostFactor),
		// It takes at least 1 and at most 4 bytes to encode a code point.
		Result: Between(Operand(0).Div(4), Operand(0)),
	},
	overloads.ExtQuoteString: {
		Traversed: Operand(0).Scale(StringTraversalCostFactor),
		// Every character may need to be escaped, and two quote characters are always added.
		Result: Between(Operand(0).Offset(2), Operand(0).Times(Const(2)).Offset(2)),
	},

	// O(min(m, n)) comparisons which stop at the first difference, or once the shorter of the
	// two inputs has been exhausted.
	overloads.StartsWithString:    comparison,
	overloads.EndsWithString:      comparison,
	overloads.Equals:              comparison,
	overloads.NotEquals:           comparison,
	overloads.LessString:          comparison,
	overloads.LessEqualsString:    comparison,
	overloads.GreaterString:       comparison,
	overloads.GreaterEqualsString: comparison,
	overloads.LessBytes:           comparison,
	overloads.LessEqualsBytes:     comparison,
	overloads.GreaterBytes:        comparison,
	overloads.GreaterEqualsBytes:  comparison,

	// O(n) scan of a list. A list of constant values could be tested in O(1), but the cost
	// system does not account for that.
	overloads.InList: {
		Traversed: Operand(1),
	},

	// O(m+n) allocation of a new string or byte sequence. In the worst case a new backing store
	// is allocated and both operands are copied into it.
	overloads.AddString: concat,
	overloads.AddBytes:  concat,

	// List concatenation is O(1), but the result size is tracked for downstream estimates.
	overloads.AddList: {
		Base:   CallCost,
		Result: Sum(Operand(0), Operand(1)),
	},

	// O(nm) search functions.
	overloads.ContainsString: {
		Traversed: Product(
			Operand(0).Scale(StringTraversalCostFactor),
			Operand(1).Scale(StringTraversalCostFactor)),
	},
	// https://swtch.com/~rsc/regexp/regexp1.html applies to the RE2 implementation used by CEL.
	//
	// The string length is offset by one so that the product of the string and the regex cannot
	// collapse to zero for an empty string matched against an expensive pattern. There is no way
	// to count the expressions in the pattern, so the guess is that each expression in a regex is
	// typically at least 4 characters long.
	overloads.MatchesString: {
		Traversed: Product(
			Operand(0).Offset(1).Scale(StringTraversalCostFactor),
			Operand(1).Scale(RegexStringLengthCostFactor)),
	},
}

// comparison describes an operation which walks two inputs in lock-step and halts as soon as the
// shorter of the two has been consumed, which covers ordering, equality, and prefix and suffix
// tests alike. Comparing two scalars costs a single unit because scalars report a size of 1.
var comparison = Model{
	Traversed: Smallest(Operand(0), Operand(1)).Scale(StringTraversalCostFactor),
}

// concat describes the allocation of a new string or byte sequence holding both operands.
var concat = Model{
	Traversed: Sum(Operand(0), Operand(1)).Scale(StringTraversalCostFactor),
	Result:    Sum(Operand(0), Operand(1)),
}
