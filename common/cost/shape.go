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
	"github.com/google/cel-go/common/types"
)

// Shape describes how large a value is and, when the value is an aggregate, how large the values
// it holds are.
//
// A shape mirrors the type it describes: a list of lists of strings has a shape whose element
// shape has an element shape of its own. That recursion is what lets a size survive being put
// into a container, taken back out, concatenated, sorted, or carried through a comprehension. A
// value which holds nothing, or whose contents are unknown, simply has no element shape.
//
// The size hints an application supplies are addressed by the same structure. A hint for
// `user.emails` describes a shape's size and a hint for `user.emails.@items` describes the size
// of its element shape, so hints and the shapes CEL derives for itself are the same thing
// arriving from two directions.
type Shape struct {
	// Size is the size of the value: unicode code points in a string, bytes in a byte sequence,
	// entries in a list or map, and 1 for a scalar.
	Size Estimate

	// Key is the shape of the keys of a map, or of the indices of a list. Nil when unknown.
	Key *Shape

	// Elem is the shape of the values held by a list or map. Nil when unknown.
	Elem *Shape

	// kind is the container kind the shape describes, when it is known to be a container. It
	// records what the type of an expression cannot when the expression is dyn-typed.
	kind types.Kind

	// empty marks a shape which describes no values at all, such as the elements of an empty
	// list. It is distinct from an unknown shape: nothing is missing, there is simply nothing
	// there, so widening against it yields the other shape unchanged.
	empty bool
}

// EmptyShape returns the shape of a value which holds nothing.
//
// It is the identity of Union, which is what allows a comprehension to accumulate into an empty
// list without the empty accumulator erasing what the loop step put into it.
func EmptyShape() Shape {
	return Shape{Size: Fixed(0), empty: true}
}

// IsEmpty reports whether the shape describes no values at all.
func (s Shape) IsEmpty() bool {
	return s.empty
}

// UnknownShape returns a shape which says nothing about the value.
func UnknownShape() Shape {
	return Shape{Size: Unknown()}
}

// ScalarShape returns the shape of a value of a known size which holds nothing.
func ScalarShape(size Estimate) Shape {
	return Shape{Size: size}
}

// ListShape returns the shape of a list of a given size whose elements have a given shape.
func ListShape(size Estimate, elem Shape) Shape {
	index := ScalarShape(Fixed(1))
	return Shape{Size: size, Key: &index, Elem: &elem, kind: types.ListKind}
}

// MapShape returns the shape of a map of a given size whose keys and values have given shapes.
func MapShape(size Estimate, key, val Shape) Shape {
	return Shape{Size: size, Key: &key, Elem: &val, kind: types.MapKind}
}

// IsKnown returns whether the shape says anything about the size of the value.
func (s Shape) IsKnown() bool {
	return s.empty || !s.Size.IsUnknown() || s.Elem != nil || s.Key != nil
}

// Elements returns the shape of the values held by an aggregate, which is unknown when the shape
// does not describe the contents.
func (s Shape) Elements() Shape {
	if s.Elem == nil {
		return UnknownShape()
	}
	return *s.Elem
}

// Keys returns the shape of the keys of a map or the indices of a list.
func (s Shape) Keys() Shape {
	if s.Key == nil {
		return UnknownShape()
	}
	return *s.Key
}

// Kind returns the container kind the shape describes, or types.UnknownKind.
func (s Shape) Kind() types.Kind {
	return s.kind
}

// Resize returns the shape with a different size, holding the same contents. It describes what
// an operation which rearranges a container without changing its elements produces.
func (s Shape) Resize(size Estimate) Shape {
	s.Size = size
	return s
}

// Refine returns the shape with anything it does not know filled in from another shape. It
// combines two descriptions of the same value rather than of two alternatives.
func (s Shape) Refine(other Shape) Shape {
	if s.empty || other.empty {
		return s
	}
	if s.Size.IsUnknown() {
		s.Size = other.Size
	}
	if s.kind == types.UnknownKind {
		s.kind = other.kind
	}
	s.Key = refineChild(s.Key, other.Key)
	s.Elem = refineChild(s.Elem, other.Elem)
	return s
}

// refineChild fills in a contained shape from another, keeping whichever of the two says more.
func refineChild(a, b *Shape) *Shape {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	refined := a.Refine(*b)
	return &refined
}

// Union returns the smallest shape which encompasses both inputs, widening the sizes at every
// level. It describes a value which may have come from either of two places.
func (s Shape) Union(other Shape) Shape {
	if s.empty {
		return other
	}
	if other.empty {
		return s
	}
	union := Shape{Size: s.Size.Union(other.Size), kind: s.kind}
	if union.kind == types.UnknownKind {
		union.kind = other.kind
	}
	union.Key = unionChild(s.Key, other.Key)
	union.Elem = unionChild(s.Elem, other.Elem)
	return union
}

// unionChild widens two contained shapes, treating an absent shape as one of unknown size so
// that a union never claims to know more about the contents than either input did.
func unionChild(a, b *Shape) *Shape {
	if a == nil && b == nil {
		return nil
	}
	if a == nil {
		widened := b.Union(UnknownShape())
		return &widened
	}
	if b == nil {
		widened := a.Union(UnknownShape())
		return &widened
	}
	union := a.Union(*b)
	return &union
}
