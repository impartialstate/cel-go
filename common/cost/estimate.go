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
	"math"

	"github.com/google/cel-go/common/ast"
	"github.com/google/cel-go/common/operators"
	"github.com/google/cel-go/common/overloads"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"github.com/google/cel-go/parser"
)

// Node describes a value the estimator is being asked about.
//
// Expr is nil for a value which does not appear in the expression on its own, such as the
// elements of a list, which are described by the path to them and their type.
type Node struct {
	// Expr is the expression which produces the value, or nil.
	Expr ast.Expr

	// Type is the deduced type of the value.
	Type *types.Type

	// Path is the field path from a variable to the value, or nil if the value is not reachable
	// from a declared variable. The first element is the variable name and subsequent elements
	// are either field names or one of '@items', '@keys', '@values'.
	Path []string
}

// EstimationContext answers questions about the expression being estimated.
//
// Everything the estimator knows is reachable through the context: the deduced types, the paths
// by which values are reached, the sizes CEL derived, and the hints the caller supplied. A cost
// function is handed the context along with the expressions it is costing, so it can ask about
// values other than the ones it was given - the elements of a list it received, for instance -
// without the estimator having to anticipate the question.
type EstimationContext interface {
	// Type returns the deduced type of an expression.
	Type(expr ast.Expr) *types.Type

	// Path returns the field path from a variable to an expression, or nil.
	Path(expr ast.Expr) []string

	// Size returns the size of the value an expression produces, or an unknown estimate.
	Size(expr ast.Expr) Estimate

	// Shape returns the size of the value an expression produces along with the sizes of the
	// values it holds, to whatever depth is known.
	Shape(expr ast.Expr) Shape

	// SizeOf returns the size of a value which the expression does not name on its own.
	SizeOf(node Node) Estimate

	// ElementType returns the type of the values held by an aggregate expression, or nil.
	ElementType(expr ast.Expr) *types.Type

	// ElementSize returns the size of the values held by an aggregate expression.
	ElementSize(expr ast.Expr) Estimate

	// Constant returns the value of an expression when it is a literal.
	Constant(expr ast.Expr) (ref.Val, bool)
}

// Estimator supplies the information CEL cannot derive on its own: the size of variable-length
// inputs, and the cost of functions CEL does not know how to cost.
type Estimator interface {
	// EstimateSize returns the size of the value the node describes, or nil when the estimator
	// has no estimate to offer.
	//
	// EstimateSize is only consulted for values whose size CEL cannot determine from the
	// expression itself.
	EstimateSize(ctx EstimationContext, node Node) *Estimate

	// EstimateCall returns the cost of a call, or nil when the estimator has no estimate to
	// offer. Operands are receiver-first for member functions.
	EstimateCall(ctx EstimationContext, function, overloadID string, operands []ast.Expr) *CallEstimate
}

// CallEstimate is the cost of a call and, when it is known, the shape of the value it produces.
type CallEstimate struct {
	// Cost of the call, including the cost of evaluating its operands.
	Cost Estimate

	// Result is the shape of the value the call produces, or nil when it is unknown. Describing
	// the shape rather than only the size is what lets the size of an element survive being
	// returned inside a container.
	Result *Shape
}

// FunctionEstimator computes the cost of a single function overload from its operands, which are
// receiver-first for member functions.
type FunctionEstimator func(ctx EstimationContext, operands []ast.Expr) *CallEstimate

// EstimatorOption configures how expression costs are estimated.
type EstimatorOption func(*coster) error

// PresenceTestHasCost determines whether presence testing has a cost of one or zero.
//
// Defaults to presence test has a cost of one.
func PresenceTestHasCost(hasCost bool) EstimatorOption {
	return func(c *coster) error {
		if hasCost {
			c.presenceTestCost = Fixed(SelectAndIdentCost)
			return nil
		}
		c.presenceTestCost = Fixed(0)
		return nil
	}
}

// OverloadEstimator binds a FunctionEstimator to a function overload id, overriding both the
// cost CEL would compute for the overload and the estimate offered by the Estimator.
func OverloadEstimator(overloadID string, estimator FunctionEstimator) EstimatorOption {
	return func(c *coster) error {
		c.overloadEstimators[overloadID] = estimator
		return nil
	}
}

// EstimateCost estimates the cost of a parsed and type checked CEL expression.
func EstimateCost(checked *ast.AST, estimator Estimator, opts ...EstimatorOption) (Estimate, error) {
	c := &coster{
		checkedAST:         checked,
		estimator:          estimator,
		overloadEstimators: map[string]FunctionEstimator{},
		exprPaths:          map[int64][]string{},
		localVars:          make(scopes),
		computedShapes:     map[int64]Shape{},
		presenceTestCost:   Fixed(SelectAndIdentCost),
		estimating:         map[int64]bool{},
	}
	for _, opt := range opts {
		if err := opt(c); err != nil {
			return Estimate{}, err
		}
	}
	return c.cost(checked.Expr()), nil
}

type coster struct {
	// exprPaths maps from Expr Id to field path.
	exprPaths map[int64][]string
	// localVars tracks the local and iteration variables assigned during evaluation.
	localVars scopes
	// computedShapes tracks the shapes derived for expressions, whether read off a literal,
	// resolved from a hint, or produced by a call.
	computedShapes map[int64]Shape

	checkedAST         *ast.AST
	estimator          Estimator
	overloadEstimators map[string]FunctionEstimator
	// presenceTestCost will either be a zero or one based on whether has() macros count against
	// cost computations.
	presenceTestCost Estimate
	// estimating tracks the expressions whose size the estimator is currently being asked for.
	estimating map[int64]bool
}

// localVar captures the shape of a local variable, if it is known
type localVar struct {
	exprID int64
	path   []string
	shape  *Shape
}

// scopes is a stack of variable name to integer id stack to handle scopes created by cel.bind()
// like macros
type scopes map[string][]*localVar

func (s scopes) push(varName string, expr ast.Expr, path []string, shape *Shape) {
	s[varName] = append(s[varName], &localVar{
		exprID: expr.ID(),
		path:   path,
		shape:  shape,
	})
}

func (s scopes) pop(varName string) {
	varStack := s[varName]
	s[varName] = varStack[:len(varStack)-1]
}

func (s scopes) peek(varName string) (*localVar, bool) {
	varStack := s[varName]
	if len(varStack) > 0 {
		return varStack[len(varStack)-1], true
	}
	return nil, false
}

func (c *coster) pushIterKey(varName string, rangeExpr ast.Expr) {
	shape := c.Shape(rangeExpr)
	subpath := "@keys"
	if c.containerKind(rangeExpr, shape) == types.ListKind {
		subpath = "@indices"
	}
	c.pushIterVar(varName, rangeExpr, subpath, shape.Keys())
}

func (c *coster) pushIterValue(varName string, rangeExpr ast.Expr) {
	shape := c.Shape(rangeExpr)
	subpath := "@values"
	if c.containerKind(rangeExpr, shape) == types.ListKind {
		subpath = "@items"
	}
	c.pushIterVar(varName, rangeExpr, subpath, shape.Elements())
}

func (c *coster) pushIterSingle(varName string, rangeExpr ast.Expr) {
	shape := c.Shape(rangeExpr)
	// Iterating a list visits its elements, while iterating a map visits its keys.
	entry, subpath := shape.Keys(), "@keys"
	if c.containerKind(rangeExpr, shape) == types.ListKind {
		entry, subpath = shape.Elements(), "@items"
	}
	c.pushIterVar(varName, rangeExpr, subpath, entry)
}

// pushIterVar binds an iteration variable to the shape of the entries it visits, which is what
// carries the size of a nested container into the body of a comprehension.
func (c *coster) pushIterVar(varName string, rangeExpr ast.Expr, subpath string, entry Shape) {
	path := append(c.getPath(rangeExpr), subpath)
	var shape *Shape
	if entry.IsKnown() {
		shape = &entry
	}
	c.localVars.push(varName, rangeExpr, path, shape)
}

// containerKind reports whether an iteration range is a list or a map, preferring the deduced
// type and falling back to the shape for expressions which are dyn-typed.
func (c *coster) containerKind(rangeExpr ast.Expr, shape Shape) types.Kind {
	if kind := c.getType(rangeExpr).Kind(); kind != types.UnknownKind && kind != types.DynKind {
		return kind
	}
	return shape.Kind()
}

func (c *coster) pushLocalVar(varName string, e ast.Expr) {
	// The binding expression may be a container, so the whole shape is propagated rather than
	// only its size.
	var shape *Shape
	if s := c.Shape(e); s.IsKnown() {
		shape = &s
	}
	c.localVars.push(varName, e, c.getPath(e), shape)
}

func (c *coster) peekLocalVar(varName string) (*localVar, bool) {
	return c.localVars.peek(varName)
}

func (c *coster) popLocalVar(varName string) {
	c.localVars.pop(varName)
}

func (c *coster) cost(e ast.Expr) Estimate {
	if e == nil {
		return Estimate{}
	}
	switch e.Kind() {
	case ast.LiteralKind:
		return Fixed(ConstCost)
	case ast.IdentKind:
		return c.costIdent(e)
	case ast.SelectKind:
		return c.costSelect(e)
	case ast.CallKind:
		return c.costCall(e)
	case ast.ListKind:
		return c.costCreateList(e)
	case ast.MapKind:
		return c.costCreateMap(e)
	case ast.StructKind:
		return c.costCreateStruct(e)
	case ast.ComprehensionKind:
		if c.isBind(e) {
			return c.costBind(e)
		}
		return c.costComprehension(e)
	default:
		return Estimate{}
	}
}

func (c *coster) costIdent(e ast.Expr) Estimate {
	identName := e.AsIdent()
	// build and track the field path
	if v, ok := c.peekLocalVar(identName); ok {
		c.addPath(e, v.path)
	} else {
		c.addPath(e, []string{identName})
	}
	return Fixed(SelectAndIdentCost)
}

func (c *coster) costSelect(e ast.Expr) Estimate {
	sel := e.AsSelect()
	var sum Estimate
	if sel.IsTestOnly() {
		// recurse, but do not add any cost
		// this is equivalent to how evalTestOnly increments the runtime cost counter
		// but does not add any additional cost for the qualifier, except here we do
		// the reverse (ident adds cost)
		sum = sum.Add(c.presenceTestCost)
		sum = sum.Add(c.relativeAttributeCost(sel.Operand()))
		return sum.Add(c.cost(sel.Operand()))
	}
	sum = sum.Add(c.cost(sel.Operand()))
	sum = sum.Add(c.relativeAttributeCost(sel.Operand()))
	switch c.getType(sel.Operand()).Kind() {
	case types.MapKind, types.StructKind, types.TypeParamKind:
		sum = sum.Add(Fixed(SelectAndIdentCost))
	}

	// build and track the field path
	c.addPath(e, append(c.getPath(sel.Operand()), sel.FieldName()))
	return sum
}

func (c *coster) costCall(e ast.Expr) Estimate {
	// Dyn is just a way to disable type-checking, so return the cost of 1 with the cost of the
	// argument.
	if dynEstimate := c.maybeUnwrapDynCall(e); dynEstimate != nil {
		return *dynEstimate
	}

	// Continue estimating the cost of all other calls.
	call := e.AsCall()
	args := call.Args()

	// Operands are receiver-first, which lets a cost model address them by position without
	// caring whether the function was called as a member or a global function.
	operands := make([]ast.Expr, 0, len(args)+1)
	operandCosts := make([]Estimate, 0, len(args)+1)
	if call.IsMemberFunction() {
		operandCosts = append(operandCosts, c.cost(call.Target()))
		operands = append(operands, call.Target())
	}
	for _, arg := range args {
		operandCosts = append(operandCosts, c.cost(arg))
		operands = append(operands, arg)
	}

	overloadIDs := c.checkedAST.GetOverloadIDs(e.ID())
	if len(overloadIDs) == 0 {
		return Estimate{}
	}
	// Pick a cost estimate range that covers all the overload cost estimation ranges
	fnCost := Estimate{Min: uint64(math.MaxUint64), Max: 0}
	var resultShape *Shape
	var indexCost Estimate
	for _, overload := range overloadIDs {
		overloadCost := c.functionCost(e, call.FunctionName(), overload, operands, operandCosts)
		fnCost = fnCost.Union(overloadCost.Cost)
		if overloadCost.Result != nil {
			if resultShape == nil {
				resultShape = overloadCost.Result
			} else {
				union := resultShape.Union(*overloadCost.Result)
				resultShape = &union
			}
		}
		// Indexing yields an entry of the container it was applied to, so the result takes the
		// shape of the container's elements and the path to them.
		switch overload {
		case overloads.IndexList, overloads.IndexMap:
			if len(args) > 0 {
				subpath := "@items"
				if overload == overloads.IndexMap {
					subpath = "@values"
				}
				c.addPath(e, append(c.getPath(args[0]), subpath))
				if entry := c.Shape(args[0]).Elements(); entry.IsKnown() {
					resultShape = &entry
				}
				indexCost = c.relativeAttributeCost(args[0])
			}
		}
	}
	c.setShape(e, resultShape)
	return fnCost.Add(indexCost)
}

func (c *coster) maybeUnwrapDynCall(e ast.Expr) *Estimate {
	call := e.AsCall()
	if call.FunctionName() != "dyn" {
		return nil
	}
	arg := call.Args()[0]
	argCost := c.cost(arg)
	// Disabling type checking must not discard what is known about the value, including the path
	// by which a hint for it would be addressed.
	c.copyShape(e, arg)
	c.addPath(e, c.getPath(arg))
	callCost := Fixed(CallCost).Add(argCost)
	return &callCost
}

func (c *coster) costCreateList(e ast.Expr) Estimate {
	var sum Estimate
	for _, elem := range e.AsList().Elements() {
		sum = sum.Add(c.cost(elem))
	}
	shape := c.listLiteralShape(e)
	c.setShape(e, &shape)
	return sum.Add(Fixed(ListCreateBaseCost))
}

// listLiteralShape describes a list written out in the expression: its size is exact and its
// elements take the shape of the widest element it holds.
func (c *coster) listLiteralShape(e ast.Expr) Shape {
	create := e.AsList()
	itemShape := EmptyShape()
	for _, elem := range create.Elements() {
		itemShape = itemShape.Union(c.Shape(elem))
	}
	return ListShape(Fixed(uint64(create.Size())), itemShape)
}

func (c *coster) costCreateMap(e ast.Expr) Estimate {
	var sum Estimate
	for _, ent := range e.AsMap().Entries() {
		entry := ent.AsMapEntry()
		sum = sum.Add(c.cost(entry.Key()))
		sum = sum.Add(c.cost(entry.Value()))
	}
	shape := c.mapLiteralShape(e)
	c.setShape(e, &shape)
	return sum.Add(Fixed(MapCreateBaseCost))
}

// mapLiteralShape describes a map written out in the expression: its size is exact and its keys
// and values take the shape of the widest key and value it holds.
func (c *coster) mapLiteralShape(e ast.Expr) Shape {
	mapVal := e.AsMap()
	keyShape, valShape := EmptyShape(), EmptyShape()
	for _, ent := range mapVal.Entries() {
		entry := ent.AsMapEntry()
		keyShape = keyShape.Union(c.Shape(entry.Key()))
		valShape = valShape.Union(c.Shape(entry.Value()))
	}
	return MapShape(Fixed(uint64(mapVal.Size())), keyShape, valShape)
}

func (c *coster) costCreateStruct(e ast.Expr) Estimate {
	msgVal := e.AsStruct()
	var sum Estimate
	for _, ent := range msgVal.Fields() {
		field := ent.AsStructField()
		sum = sum.Add(c.cost(field.Value()))
	}
	return sum.Add(Fixed(StructCreateBaseCost))
}

func (c *coster) costComprehension(e ast.Expr) Estimate {
	comp := e.AsComprehension()
	var sum Estimate
	sum = sum.Add(c.cost(comp.IterRange()))
	sum = sum.Add(c.cost(comp.AccuInit()))
	c.pushLocalVar(comp.AccuVar(), comp.AccuInit())

	// Track the iterRange of each IterVar and AccuVar for field path construction
	if comp.HasIterVar2() {
		c.pushIterKey(comp.IterVar(), comp.IterRange())
		c.pushIterValue(comp.IterVar2(), comp.IterRange())
	} else {
		c.pushIterSingle(comp.IterVar(), comp.IterRange())
	}

	// Determine the cost for each element in the loop
	loopCost := c.cost(comp.LoopCondition())
	stepCost := c.cost(comp.LoopStep())

	// Clear the intermediate variable tracking.
	c.popLocalVar(comp.IterVar())
	if comp.HasIterVar2() {
		c.popLocalVar(comp.IterVar2())
	}

	// Determine the result cost.
	sum = sum.Add(c.cost(comp.Result()))
	c.localVars.pop(comp.AccuVar())

	// Estimate the cost of the loop.
	rangeCnt := c.Size(comp.IterRange())
	sum = sum.Add(rangeCnt.Multiply(stepCost.Add(loopCost)))

	switch comp.AccuInit().Kind() {
	case ast.LiteralKind:
		c.copyShape(e, comp.AccuInit())
	case ast.ListKind, ast.MapKind:
		// A comprehension accumulates into a container, so the result holds whatever the loop
		// step accumulated, once per iteration of the range.
		shape := c.Shape(comp.LoopStep()).Resize(rangeCnt)
		c.setShape(e, &shape)
	}
	return sum
}

func (c *coster) isBind(e ast.Expr) bool {
	comp := e.AsComprehension()
	iterRange := comp.IterRange()
	loopCond := comp.LoopCondition()
	return iterRange.Kind() == ast.ListKind && iterRange.AsList().Size() == 0 &&
		loopCond.Kind() == ast.LiteralKind && loopCond.AsLiteral() == types.False &&
		comp.AccuVar() != parser.AccumulatorName
}

func (c *coster) costBind(e ast.Expr) Estimate {
	comp := e.AsComprehension()
	var sum Estimate
	// Binds are lazily initialized, so we retain the cost of an empty iteration range.
	sum = sum.Add(c.cost(comp.IterRange()))
	sum = sum.Add(c.cost(comp.AccuInit()))

	c.pushLocalVar(comp.AccuVar(), comp.AccuInit())
	sum = sum.Add(c.cost(comp.Result()))
	c.popLocalVar(comp.AccuVar())

	// Associate the bind output shape with the result shape.
	c.copyShape(e, comp.Result())
	return sum
}

// functionCost computes the cost of a single overload, including the cost of evaluating the
// operands which were passed to it.
func (c *coster) functionCost(e ast.Expr, function, overloadID string, operands []ast.Expr, operandCosts []Estimate) CallEstimate {
	operandCostSum := func() Estimate {
		var sum Estimate
		for _, a := range operandCosts {
			sum = sum.Add(a)
		}
		return sum
	}
	withOperands := func(est *CallEstimate) CallEstimate {
		return CallEstimate{Cost: est.Cost.Add(operandCostSum()), Result: est.Result}
	}
	// A registered estimator for the overload takes precedence over everything else.
	if estimator, found := c.overloadEstimators[overloadID]; found {
		if est := estimator(c, operands); est != nil {
			return withOperands(est)
		}
	}
	if c.estimator != nil {
		if est := c.estimator.EstimateCall(c, function, overloadID, operands); est != nil {
			return withOperands(est)
		}
	}
	// Overloads whose cost depends on the structure of the expression rather than only on the
	// sizes of the operands.
	switch overloadID {
	case overloads.LogicalOr, overloads.LogicalAnd:
		lhs := operandCosts[0]
		rhs := operandCosts[1]
		// min cost is min of LHS for short circuited && or ||
		return CallEstimate{Cost: Estimate{Min: lhs.Min, Max: lhs.Add(rhs).Max}}
	case overloads.Conditional:
		// The result came from one branch or the other, so it is shaped like either.
		shape := c.Shape(operands[1]).Union(c.Shape(operands[2]))
		argCost := operandCosts[0].Add(operandCosts[1].Union(operandCosts[2]))
		return CallEstimate{Cost: argCost, Result: &shape}
	}
	// Every remaining function is described by a cost model, defaulting to a call which does no
	// work proportional to the size of its inputs.
	model, found := StandardModel(overloadID)
	if !found {
		model = Model{Base: CallCost}
	}
	return withOperands(model.Estimate(c, operands))
}

// relativeAttributeCost is the cost of qualifying a value which was computed rather than named.
//
// Evaluation reaches into a value by resolving an attribute and then applying qualifiers to it.
// Resolving the attribute costs the same as resolving an identifier, and when the value being
// qualified is named, that is exactly what it is: the cost is already charged to the identifier.
// A value which was computed has no identifier to charge it to, so it is charged here, once for
// the whole chain of qualifiers applied to it.
func (c *coster) relativeAttributeCost(operand ast.Expr) Estimate {
	if isAttributeChain(operand) {
		return Estimate{}
	}
	return Fixed(SelectAndIdentCost)
}

// isAttributeChain reports whether an expression is resolved as part of a single attribute during
// evaluation. A chain begins at an identifier, or at a ternary which selects between attributes,
// and is extended by field selections and index operations.
func isAttributeChain(e ast.Expr) bool {
	switch e.Kind() {
	case ast.IdentKind, ast.SelectKind:
		return true
	case ast.CallKind:
		switch e.AsCall().FunctionName() {
		case operators.Index, operators.Conditional:
			return true
		}
	}
	return false
}

func (c *coster) getType(e ast.Expr) *types.Type {
	return c.checkedAST.GetType(e.ID())
}

func (c *coster) getPath(e ast.Expr) []string {
	if e.Kind() == ast.IdentKind {
		if v, found := c.peekLocalVar(e.AsIdent()); found {
			return v.path[:]
		}
	}
	return c.exprPaths[e.ID()][:]
}

func (c *coster) addPath(e ast.Expr, path []string) {
	c.exprPaths[e.ID()] = path
}

func isAccumulatorVar(name string) bool {
	return name == parser.AccumulatorName || name == parser.HiddenAccumulatorName
}

// The coster answers the questions of the EstimationContext interface, since it is the only
// thing which holds the checked expression, the paths, the size estimates, and the caller's
// estimator all at once.

// Type implements the EstimationContext interface method.
func (c *coster) Type(e ast.Expr) *types.Type {
	return c.getType(e)
}

// Path implements the EstimationContext interface method.
func (c *coster) Path(e ast.Expr) []string {
	path := c.getPath(e)
	if len(path) > 0 && isAccumulatorVar(path[0]) {
		// only provide paths to root vars; omit accumulator vars
		return nil
	}
	return path
}

// Size implements the EstimationContext interface method.
func (c *coster) Size(e ast.Expr) Estimate {
	return c.Shape(e).Size
}

// SizeOf implements the EstimationContext interface method.
func (c *coster) SizeOf(n Node) Estimate {
	if n.Expr != nil {
		return c.Size(n.Expr)
	}
	shape, _ := c.hintedShape(n.Path, n.Type, 0)
	return shape.Size
}

// ElementType implements the EstimationContext interface method.
func (c *coster) ElementType(e ast.Expr) *types.Type {
	return elemType(c.getType(e))
}

// ElementSize implements the EstimationContext interface method.
func (c *coster) ElementSize(e ast.Expr) Estimate {
	return c.Shape(e).Elements().Size
}

// Constant implements the EstimationContext interface method.
func (c *coster) Constant(e ast.Expr) (ref.Val, bool) {
	if e == nil || e.Kind() != ast.LiteralKind {
		return nil, false
	}
	return e.AsLiteral(), true
}

// Shape implements the EstimationContext interface method.
//
// Shapes are resolved in order of how much they are worth: what an earlier step recorded for the
// expression, what the expression says about itself, what the application hinted for the path to
// it, and finally what its type alone implies.
func (c *coster) Shape(e ast.Expr) Shape {
	if e == nil {
		return UnknownShape()
	}
	if shape, found := c.computedShapes[e.ID()]; found {
		return shape
	}
	if shape, found := c.literalShape(e); found {
		c.computedShapes[e.ID()] = shape
		return shape
	}
	shape, found := c.hintedShape(c.Path(e), c.getType(e), 0)
	if e.Kind() == ast.IdentKind {
		// A local variable is described by the expression it was bound to, or by the entries of
		// the range it iterates, which says more than anything addressed by path alone. Hints
		// only fill in what the binding does not say.
		if v, ok := c.peekLocalVar(e.AsIdent()); ok && v.shape != nil {
			return v.shape.Refine(shape)
		}
		// The shape of an identifier depends on the scope it is resolved in, so it is not
		// recorded against the expression.
		return shape
	}
	if found {
		c.computedShapes[e.ID()] = shape
	}
	return shape
}

// literalShape reads the shape of a value which is written out in the expression itself.
func (c *coster) literalShape(e ast.Expr) (Shape, bool) {
	switch e.Kind() {
	case ast.LiteralKind:
		var size uint64
		switch lit := e.AsLiteral().(type) {
		case types.String:
			// converting to runes here is an O(n) operation, but this is consistent with how
			// size is computed at runtime, and how the language definition defines string size
			size = uint64(len([]rune(lit)))
		case types.Bytes:
			size = uint64(len(lit))
		case types.Bool, types.Double, types.Duration,
			types.Int, types.Timestamp, types.Uint,
			types.Null:
			size = 1
		default:
			return UnknownShape(), false
		}
		return ScalarShape(Fixed(size)), true
	case ast.ListKind:
		return c.listLiteralShape(e), true
	case ast.MapKind:
		return c.mapLiteralShape(e), true
	}
	return UnknownShape(), false
}

// maxShapeDepth bounds how far a shape is resolved from hints. Type nesting terminates on its
// own, so the limit only guards against a pathological type.
const maxShapeDepth = 8

// hintedShape resolves the shape of a value from the sizes an application hinted for the path to
// it, descending into the contents of a container so that a hint for `x.@items` describes the
// elements of `x` and `x.@items.@items` the elements of those.
//
// Where no hint is offered the type is consulted, which pins the size of every scalar.
func (c *coster) hintedShape(path []string, t *types.Type, depth int) (Shape, bool) {
	if depth > maxShapeDepth {
		return UnknownShape(), false
	}
	shape, found := UnknownShape(), false
	if c.estimator != nil {
		if size := c.askEstimator(Node{Path: path, Type: t}); size != nil {
			shape.Size, found = *size, true
		}
	}
	if t == nil {
		return shape, found
	}
	switch t.Kind() {
	case types.ListKind:
		shape.kind = types.ListKind
		index := ScalarShape(Fixed(1))
		shape.Key = &index
		if elem, ok := c.hintedShape(subpath(path, "@items"), t.Parameters()[0], depth+1); ok {
			shape.Elem, found = &elem, true
		}
	case types.MapKind:
		shape.kind = types.MapKind
		if key, ok := c.hintedShape(subpath(path, "@keys"), t.Parameters()[0], depth+1); ok {
			shape.Key, found = &key, true
		}
		if val, ok := c.hintedShape(subpath(path, "@values"), t.Parameters()[1], depth+1); ok {
			shape.Elem, found = &val, true
		}
	default:
		if !found {
			if size := typeSize(t); size != nil {
				shape.Size, found = *size, true
			}
		}
	}
	return shape, found
}

// subpath extends a field path without sharing its backing array.
func subpath(path []string, sub string) []string {
	if len(path) == 0 {
		return nil
	}
	return append(append(make([]string, 0, len(path)+1), path...), sub)
}

// askEstimator consults the application for the size of a value, guarding against an estimator
// which asks the context about the very value it is being asked to size.
func (c *coster) askEstimator(n Node) *Estimate {
	var id int64
	if n.Expr != nil {
		id = n.Expr.ID()
		if c.estimating[id] {
			return nil
		}
		c.estimating[id] = true
		defer delete(c.estimating, id)
	}
	return c.estimator.EstimateSize(c, n)
}

func (c *coster) setShape(e ast.Expr, shape *Shape) {
	if shape == nil {
		return
	}
	c.computedShapes[e.ID()] = *shape
}

func (c *coster) copyShape(dst, src ast.Expr) {
	if shape := c.Shape(src); shape.IsKnown() {
		c.computedShapes[dst.ID()] = shape
	}
}

// typeSize returns the size implied by a type alone, which is known only for fixed width values.
func typeSize(t *types.Type) *Estimate {
	if isScalar(t) {
		size := Fixed(1)
		return &size
	}
	return nil
}

// isScalar returns true if the given type is known to be of a constant size at
// compile time. isScalar will return false for strings (they are variable-width)
// in addition to protobuf.Any and protobuf.Value (their size is not knowable at compile time).
func isScalar(t *types.Type) bool {
	switch t.Kind() {
	case types.BoolKind, types.DoubleKind, types.DurationKind, types.IntKind, types.TimestampKind, types.UintKind:
		return true
	case types.OpaqueKind:
		if t.TypeName() == "optional_type" {
			return isScalar(t.Parameters()[0])
		}
	}
	return false
}
