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

// CallEstimate is the cost of a call and, when the call produces an aggregate value, the size of
// its result.
type CallEstimate struct {
	// Cost of the call, including the cost of evaluating its operands.
	Cost Estimate

	// ResultSize is the size of the value the call produces, or nil when it is unknown.
	ResultSize *Estimate
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
		computedSizes:      map[int64]Estimate{},
		computedEntrySizes: map[int64]entrySize{},
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
	// computedSizes tracks the computed sizes of call results.
	computedSizes map[int64]Estimate
	// computedEntrySizes tracks the size of list and map entries
	computedEntrySizes map[int64]entrySize

	checkedAST         *ast.AST
	estimator          Estimator
	overloadEstimators map[string]FunctionEstimator
	// presenceTestCost will either be a zero or one based on whether has() macros count against
	// cost computations.
	presenceTestCost Estimate
	// estimating tracks the expressions whose size the estimator is currently being asked for.
	estimating map[int64]bool
}

// entrySize captures the container kind and associated key/index and value size estimates.
//
// An entrySize only exists if both the key/index and the value have size estimates, otherwise a
// nil entrySize should be used.
type entrySize struct {
	containerKind types.Kind
	key           Estimate
	val           Estimate
}

// container returns the container kind (list or map) of the entry.
func (s *entrySize) container() types.Kind {
	if s == nil {
		return types.UnknownKind
	}
	return s.containerKind
}

// keySize returns the size estimate for the key if one exists.
func (s *entrySize) keySize() *Estimate {
	if s == nil {
		return nil
	}
	return &s.key
}

// valSize returns the size estimate for the value if one exists.
func (s *entrySize) valSize() *Estimate {
	if s == nil {
		return nil
	}
	return &s.val
}

func (s *entrySize) union(other *entrySize) *entrySize {
	if s == nil || other == nil {
		return nil
	}
	return &entrySize{
		containerKind: s.containerKind,
		key:           s.key.Union(other.key),
		val:           s.val.Union(other.val),
	}
}

// localVar captures the local variable size and entry size estimates if they exist for variables
type localVar struct {
	exprID    int64
	path      []string
	size      *Estimate
	entrySize *entrySize
}

// scopes is a stack of variable name to integer id stack to handle scopes created by cel.bind()
// like macros
type scopes map[string][]*localVar

func (s scopes) push(varName string, expr ast.Expr, path []string, size *Estimate, entry *entrySize) {
	s[varName] = append(s[varName], &localVar{
		exprID:    expr.ID(),
		path:      path,
		size:      size,
		entrySize: entry,
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
	entry := c.computeEntrySize(rangeExpr)
	size := entry.keySize()
	path := c.getPath(rangeExpr)
	container := entry.container()
	if container == types.UnknownKind {
		container = c.getType(rangeExpr).Kind()
	}
	subpath := "@keys"
	if container == types.ListKind {
		subpath = "@indices"
	}
	c.localVars.push(varName, rangeExpr, append(path, subpath), size, nil)
}

func (c *coster) pushIterValue(varName string, rangeExpr ast.Expr) {
	entry := c.computeEntrySize(rangeExpr)
	size := entry.valSize()
	path := c.getPath(rangeExpr)
	container := entry.container()
	if container == types.UnknownKind {
		container = c.getType(rangeExpr).Kind()
	}
	subpath := "@values"
	if container == types.ListKind {
		subpath = "@items"
	}
	c.localVars.push(varName, rangeExpr, append(path, subpath), size, nil)
}

func (c *coster) pushIterSingle(varName string, rangeExpr ast.Expr) {
	entry := c.computeEntrySize(rangeExpr)
	size := entry.keySize()
	subpath := "@keys"
	container := entry.container()
	if container == types.UnknownKind {
		container = c.getType(rangeExpr).Kind()
	}
	if container == types.ListKind {
		size = entry.valSize()
		subpath = "@items"
	}
	path := c.getPath(rangeExpr)
	c.localVars.push(varName, rangeExpr, append(path, subpath), size, nil)
}

func (c *coster) pushLocalVar(varName string, e ast.Expr) {
	path := c.getPath(e)
	// note: retrieve the entry size for the local variable based on the size of the binding
	// expression since the binding expression could be a list or map, the entry size should also
	// be propagated
	c.localVars.push(varName, e, path, c.computeSize(e), c.computeEntrySize(e))
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
		return sum.Add(c.cost(sel.Operand()))
	}
	sum = sum.Add(c.cost(sel.Operand()))
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
	var resultSize *Estimate
	for _, overload := range overloadIDs {
		overloadCost := c.functionCost(e, call.FunctionName(), overload, operands, operandCosts)
		fnCost = fnCost.Union(overloadCost.Cost)
		if overloadCost.ResultSize != nil {
			if resultSize == nil {
				resultSize = overloadCost.ResultSize
			} else {
				size := resultSize.Union(*overloadCost.ResultSize)
				resultSize = &size
			}
		}
		// build and track the field path for index operations
		switch overload {
		case overloads.IndexList:
			if len(args) > 0 {
				// note: assigning resultSize here could be redundant with the path-based lookup
				// later
				resultSize = c.computeEntrySize(args[0]).valSize()
				c.addPath(e, append(c.getPath(args[0]), "@items"))
			}
		case overloads.IndexMap:
			if len(args) > 0 {
				resultSize = c.computeEntrySize(args[0]).valSize()
				c.addPath(e, append(c.getPath(args[0]), "@values"))
			}
		}
		if resultSize == nil {
			resultSize = c.computeSize(e)
		}
	}
	c.setSize(e, resultSize)
	return fnCost
}

func (c *coster) maybeUnwrapDynCall(e ast.Expr) *Estimate {
	call := e.AsCall()
	if call.FunctionName() != "dyn" {
		return nil
	}
	arg := call.Args()[0]
	argCost := c.cost(arg)
	c.copySizeEstimates(e, arg)
	callCost := Fixed(CallCost).Add(argCost)
	return &callCost
}

func (c *coster) costCreateList(e ast.Expr) Estimate {
	create := e.AsList()
	var sum Estimate
	itemSize := Estimate{Min: math.MaxUint64, Max: 0}
	if create.Size() == 0 {
		itemSize.Min = 0
	}
	for _, e := range create.Elements() {
		sum = sum.Add(c.cost(e))
		itemSize = itemSize.Union(c.sizeOrUnknown(e))
	}
	c.setEntrySize(e, &entrySize{containerKind: types.ListKind, key: Fixed(1), val: itemSize})
	return sum.Add(Fixed(ListCreateBaseCost))
}

func (c *coster) costCreateMap(e ast.Expr) Estimate {
	mapVal := e.AsMap()
	var sum Estimate
	keySize := Estimate{Min: math.MaxUint64, Max: 0}
	valSize := Estimate{Min: math.MaxUint64, Max: 0}
	if mapVal.Size() == 0 {
		valSize.Min = 0
		keySize.Min = 0
	}
	for _, ent := range mapVal.Entries() {
		entry := ent.AsMapEntry()
		sum = sum.Add(c.cost(entry.Key()))
		sum = sum.Add(c.cost(entry.Value()))
		keySize = keySize.Union(c.sizeOrUnknown(entry.Key()))
		valSize = valSize.Union(c.sizeOrUnknown(entry.Value()))
	}
	c.setEntrySize(e, &entrySize{containerKind: types.MapKind, key: keySize, val: valSize})
	return sum.Add(Fixed(MapCreateBaseCost))
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
	rangeCnt := c.sizeOrUnknown(comp.IterRange())
	sum = sum.Add(rangeCnt.Multiply(stepCost.Add(loopCost)))

	switch comp.AccuInit().Kind() {
	case ast.LiteralKind:
		c.setSize(e, c.computeSize(comp.AccuInit()))
	case ast.ListKind, ast.MapKind:
		c.setSize(e, &rangeCnt)
		// For a step which produces a container value, it will have an entry size associated
		// with its expression id.
		if stepEntrySize := c.computeEntrySize(comp.LoopStep()); stepEntrySize != nil {
			c.setEntrySize(e, stepEntrySize)
		}
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

	// Associate the bind output size with the result size.
	c.copySizeEstimates(e, comp.Result())
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
		return CallEstimate{Cost: est.Cost.Add(operandCostSum()), ResultSize: est.ResultSize}
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
		size := c.Size(operands[1]).Union(c.Size(operands[2]))
		c.setEntrySize(e, c.computeEntrySize(operands[1]).union(c.computeEntrySize(operands[2])))
		argCost := operandCosts[0].Add(operandCosts[1].Union(operandCosts[2]))
		return CallEstimate{Cost: argCost, ResultSize: &size}
	case overloads.AddString, overloads.AddBytes, overloads.AddList:
		// Concatenation propagates the entry sizes of its operands to the result.
		if entry := c.computeEntrySize(operands[0]).union(c.computeEntrySize(operands[1])); entry != nil {
			c.setEntrySize(e, entry)
		}
	}
	// Every remaining function is described by a cost model, defaulting to a call which does no
	// work proportional to the size of its inputs.
	model, found := StandardModel(overloadID)
	if !found {
		model = Model{Base: CallCost}
	}
	return withOperands(model.Estimate(c, operands))
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
	return c.sizeOrUnknown(e)
}

// SizeOf implements the EstimationContext interface method.
func (c *coster) SizeOf(n Node) Estimate {
	if n.Expr != nil {
		return c.Size(n.Expr)
	}
	if c.estimator != nil {
		if size := c.estimator.EstimateSize(c, n); size != nil {
			return *size
		}
	}
	if size := computeTypeSize(n.Type); size != nil {
		return *size
	}
	return Unknown()
}

// ElementType implements the EstimationContext interface method.
func (c *coster) ElementType(e ast.Expr) *types.Type {
	return elemType(c.getType(e))
}

// ElementSize implements the EstimationContext interface method.
//
// The size comes from the contents of the expression where CEL can see them, and otherwise from
// the caller's estimate for the path to the elements.
func (c *coster) ElementSize(e ast.Expr) Estimate {
	if entry := c.computeEntrySize(e); entry != nil {
		return entry.val
	}
	elemType := c.ElementType(e)
	if elemType == nil {
		return Unknown()
	}
	var path []string
	if p := c.Path(e); len(p) != 0 {
		subpath := "@items"
		if c.getType(e).Kind() == types.MapKind {
			subpath = "@values"
		}
		path = append(append(make([]string, 0, len(p)+1), p...), subpath)
	}
	return c.SizeOf(Node{Type: elemType, Path: path})
}

// Constant implements the EstimationContext interface method.
func (c *coster) Constant(e ast.Expr) (ref.Val, bool) {
	if e == nil || e.Kind() != ast.LiteralKind {
		return nil, false
	}
	return e.AsLiteral(), true
}

func (c *coster) setSize(e ast.Expr, size *Estimate) {
	if size == nil {
		return
	}
	// Store the computed size with the expression
	c.computedSizes[e.ID()] = *size
}

func (c *coster) sizeOrUnknown(e ast.Expr) Estimate {
	if sz := c.computeSize(e); sz != nil {
		return *sz
	}
	return Unknown()
}

func (c *coster) copySizeEstimates(dst, src ast.Expr) {
	c.setSize(dst, c.computeSize(src))
	c.setEntrySize(dst, c.computeEntrySize(src))
}

func (c *coster) computeSize(e ast.Expr) *Estimate {
	if size, ok := c.computedSizes[e.ID()]; ok {
		return &size
	}
	if size := computeExprSize(e); size != nil {
		return size
	}
	// Ensure size estimates are computed first as users may choose to override the costs that
	// CEL would otherwise ascribe to the type.
	if c.estimator != nil && !c.estimating[e.ID()] {
		// An estimator is handed the context, and may ask it about the very expression it is
		// being asked to size, so the question is only asked once.
		c.estimating[e.ID()] = true
		size := c.estimator.EstimateSize(c, Node{Expr: e, Path: c.Path(e), Type: c.getType(e)})
		delete(c.estimating, e.ID())
		if size != nil {
			// storing the computed size should reduce calls to EstimateSize()
			c.computedSizes[e.ID()] = *size
			return size
		}
	}
	if size := computeTypeSize(c.getType(e)); size != nil {
		return size
	}
	if e.Kind() == ast.IdentKind {
		if v, ok := c.peekLocalVar(e.AsIdent()); ok && v.size != nil {
			return v.size
		}
	}
	return nil
}

func (c *coster) setEntrySize(e ast.Expr, size *entrySize) {
	if size == nil {
		return
	}
	c.computedEntrySizes[e.ID()] = *size
}

func (c *coster) computeEntrySize(e ast.Expr) *entrySize {
	if sz, found := c.computedEntrySizes[e.ID()]; found {
		return &sz
	}
	if e.Kind() == ast.IdentKind {
		if v, ok := c.peekLocalVar(e.AsIdent()); ok && v.entrySize != nil {
			return v.entrySize
		}
	}
	return nil
}

func computeExprSize(expr ast.Expr) *Estimate {
	var v uint64
	switch expr.Kind() {
	case ast.LiteralKind:
		switch ck := expr.AsLiteral().(type) {
		case types.String:
			// converting to runes here is an O(n) operation, but
			// this is consistent with how size is computed at runtime,
			// and how the language definition defines string size
			v = uint64(len([]rune(ck)))
		case types.Bytes:
			v = uint64(len(ck))
		case types.Bool, types.Double, types.Duration,
			types.Int, types.Timestamp, types.Uint,
			types.Null:
			v = uint64(1)
		default:
			return nil
		}
	case ast.ListKind:
		v = uint64(expr.AsList().Size())
	case ast.MapKind:
		v = uint64(expr.AsMap().Size())
	default:
		return nil
	}
	size := Fixed(v)
	return &size
}

func computeTypeSize(t *types.Type) *Estimate {
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
