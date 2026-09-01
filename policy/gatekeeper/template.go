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

// Package gatekeeper compiles and tests the CEL within OPA Gatekeeper
// ConstraintTemplates without a Kubernetes cluster.
//
// Gatekeeper's K8sNativeValidation engine embeds CEL directly in a
// ConstraintTemplate:
//
//	apiVersion: templates.gatekeeper.sh/v1
//	kind: ConstraintTemplate
//	metadata:
//	  name: k8srequiredlabels
//	spec:
//	  crd:
//	    spec:
//	      names:
//	        kind: K8sRequiredLabels
//	      validation:
//	        openAPIV3Schema: ...
//	  targets:
//	    - target: admission.k8s.gatekeeper.sh
//	      code:
//	        - engine: K8sNativeValidation
//	          source:
//	            variables:
//	              - name: labels
//	                expression: "variables.params.labels"
//	            validations:
//	              - expression: "..."
//	                messageExpression: "..."
//
// This package parses that document into a CEL policy so the expressions can be
// type-checked and unit tested locally, with errors reported against the
// template's own line and column numbers. It also gives CEL policies the
// referential data Rego policies read from data.inventory and fetch with
// external_data. See the README for the whole picture.
//
// The environment a policy is compiled against tracks what a Kubernetes cluster
// provides rather than everything cel-go offers, so that a template which
// compiles here is one a cluster will accept. The libraries a recent cluster
// has and this environment does not are quantity, URL, IP and CIDR, format,
// semver, the authorizer, and two-variable comprehensions. A policy which needs
// one of these can declare it with the EnvOptions option, and a policy which
// needs quantity or the authorizer needs an implementation of it first.
//
// What a cluster decides and this package does not: whether a constraint
// applies to an object at all, which is the constraint's spec.match; what is
// done with a violation, which is its enforcementAction and the template's
// failurePolicy; and the cost limits which bound how long an expression may run.
package gatekeeper

import (
	"fmt"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"

	"cel.dev/cel-go/policy"
)

const (
	// EngineName is the Gatekeeper engine which evaluates CEL.
	EngineName = "K8sNativeValidation"

	// RegoEngineName is the Gatekeeper engine which evaluates Rego.
	RegoEngineName = "Rego"

	// AdmissionTarget is the Gatekeeper target for Kubernetes admission review.
	AdmissionTarget = "admission.k8s.gatekeeper.sh"

	// APIGroup is the API group for ConstraintTemplate resources.
	APIGroup = "templates.gatekeeper.sh"

	// TemplateKind is the Kubernetes kind of a Gatekeeper policy template.
	TemplateKind = "ConstraintTemplate"
)

// Metadata keys set on the parsed policy. The values are consumed when
// configuring the CEL environment for the template.
const (
	// MetadataAPIVersion holds the template's apiVersion string.
	MetadataAPIVersion = "gatekeeper.apiVersion"

	// MetadataConstraintKind holds spec.crd.spec.names.kind, the kind of the
	// constraints which instantiate the template.
	MetadataConstraintKind = "gatekeeper.constraintKind"

	// MetadataParamsSchema holds spec.crd.spec.validation.openAPIV3Schema as a
	// generic map, describing the shape of `variables.params`.
	MetadataParamsSchema = "gatekeeper.paramsSchema"

	// MetadataGenerateVAP holds the source's generateVAP setting, if present.
	MetadataGenerateVAP = "gatekeeper.generateVAP"

	// MetadataHasRegoEngine reports whether the template also carries a Rego
	// implementation of the same policy.
	MetadataHasRegoEngine = "gatekeeper.hasRegoEngine"

	// MetadataFailurePolicy holds the source's failurePolicy setting, if present.
	MetadataFailurePolicy = "gatekeeper.failurePolicy"

	// MetadataTemplateName holds the template's metadata.name, which names the
	// types derived from the schemas it declares.
	MetadataTemplateName = "gatekeeper.templateName"
)

// defaultMessage is the placeholder output assigned to a validation while it is
// being parsed. Validations which declare neither `message` nor
// `messageExpression` have it replaced with a message naming the expression
// which failed, mirroring the Kubernetes admission behavior.
const defaultMessage = "@gatekeeper.defaultMessage"

// matchConditionPrefix names the variables which hold a source's match
// conditions.
const matchConditionPrefix = "matchCondition"

// ParserOption configures a policy parser to read Gatekeeper ConstraintTemplates.
//
// The option is passed to policy.NewParser, or to any tool which accepts
// policy.ParserOption values, such as the CEL test runner:
//
//	parser, err := policy.NewParser(gatekeeper.ParserOption())
//
// The options must match those given to EnvOptionFromMetadata, as together they
// determine which variables the template declares and which the environment
// does.
func ParserOption(opts ...Option) policy.ParserOption {
	return func(p *policy.Parser) (*policy.Parser, error) {
		p.TagVisitor = TagHandler(opts...)
		return p, nil
	}
}

// TagHandler returns a policy.TagVisitor which reads the Kubernetes and
// Gatekeeper specific tags of a ConstraintTemplate document.
func TagHandler(opts ...Option) policy.TagVisitor {
	return &templateTagHandler{
		TagVisitor: policy.DefaultTagVisitor(),
		config:     newConfig(opts...),
	}
}

// templateTagHandler parses one ConstraintTemplate at a time, tracking the
// match conditions of the template being parsed so that they can be applied as
// a guard over its validations.
type templateTagHandler struct {
	policy.TagVisitor

	config          *config
	matchConditions []policy.ValueString
	celFound        bool
}

// PolicyTag handles the top-level fields of a ConstraintTemplate document:
// apiVersion, kind, metadata, and spec.
func (h *templateTagHandler) PolicyTag(ctx policy.ParserContext, id int64, tagName string, node *yaml.Node, p *policy.Policy) {
	switch tagName {
	case "apiVersion":
		version := ctx.NewString(node)
		p.SetMetadata(MetadataAPIVersion, version.Value)
		if group, _, found := strings.Cut(version.Value, "/"); !found || group != APIGroup {
			ctx.ReportErrorAtID(version.ID, "unsupported apiVersion: %s, wanted an %s version", version.Value, APIGroup)
		}
	case "kind":
		kind := ctx.NewString(node)
		if kind.Value != TemplateKind {
			ctx.ReportErrorAtID(kind.ID, "unsupported kind: %s, wanted %s", kind.Value, TemplateKind)
		}
	case "metadata":
		h.parseMetadata(ctx, node, p)
	case "spec":
		h.parseSpec(ctx, id, node, p)
	default:
		ctx.ReportErrorAtID(id, "unsupported ConstraintTemplate field: %s", tagName)
	}
}

// parseMetadata reads the template name, which names the policy, and the
// description annotation if one is present.
func (h *templateTagHandler) parseMetadata(ctx policy.ParserContext, node *yaml.Node, p *policy.Policy) {
	rangeMap(ctx, node, func(fieldName string, keyID int64, val *yaml.Node) {
		switch fieldName {
		case "name":
			name := ctx.NewString(val)
			p.SetName(name)
			p.SetMetadata(MetadataTemplateName, name.Value)
		case "annotations":
			rangeMap(ctx, val, func(annotation string, _ int64, val *yaml.Node) {
				if annotation == "description" {
					p.SetDescription(ctx.NewString(val))
				}
			})
		}
	})
}

// parseSpec reads the constraint CRD and locates the CEL code within the
// template's targets.
func (h *templateTagHandler) parseSpec(ctx policy.ParserContext, specID int64, node *yaml.Node, p *policy.Policy) {
	h.matchConditions = nil
	h.celFound = false
	celFound := false
	rangeMap(ctx, node, func(fieldName string, keyID int64, val *yaml.Node) {
		switch fieldName {
		case "crd":
			h.parseCRD(ctx, keyID, val, p)
		case "targets":
			celFound = h.parseTargets(ctx, keyID, val, p) || celFound
		default:
			ctx.ReportErrorAtID(keyID, "unsupported spec field: %s", fieldName)
		}
	})
	if !celFound {
		engines := "no policy engine"
		if hasRego, found := p.Metadata(MetadataHasRegoEngine); found && hasRego == true {
			engines = RegoEngineName
		}
		ctx.ReportErrorAtID(specID,
			"no %s code found in spec.targets: this template is implemented with %s, "+
				"and only CEL policies can be compiled", EngineName, engines)
	}
}

// parseCRD records the constraint kind and the openAPIV3Schema describing the
// parameters accepted by constraints of this template.
func (h *templateTagHandler) parseCRD(ctx policy.ParserContext, crdID int64, node *yaml.Node, p *policy.Policy) {
	rangeMap(ctx, node, func(fieldName string, keyID int64, val *yaml.Node) {
		if fieldName != "spec" {
			return
		}
		rangeMap(ctx, val, func(specField string, specKeyID int64, specVal *yaml.Node) {
			switch specField {
			case "names":
				rangeMap(ctx, specVal, func(nameField string, _ int64, nameVal *yaml.Node) {
					if nameField == "kind" {
						p.SetMetadata(MetadataConstraintKind, nameVal.Value)
					}
				})
			case "validation":
				rangeMap(ctx, specVal, func(validationField string, validationKeyID int64, schemaVal *yaml.Node) {
					if validationField != "openAPIV3Schema" {
						return
					}
					schema := map[string]any{}
					if err := schemaVal.Decode(&schema); err != nil {
						ctx.ReportErrorAtID(validationKeyID, "invalid openAPIV3Schema: %v", err)
						return
					}
					p.SetMetadata(MetadataParamsSchema, schema)
				})
			}
		})
	})
}

// parseTargets locates the K8sNativeValidation source within the template's
// targets and parses it as the policy rule. It reports whether CEL code was
// found.
func (h *templateTagHandler) parseTargets(ctx policy.ParserContext, targetsID int64, node *yaml.Node, p *policy.Policy) bool {
	if !assertList(ctx, targetsID, "targets", node) {
		return false
	}
	celFound := false
	for _, target := range node.Content {
		rangeMap(ctx, target, func(fieldName string, keyID int64, val *yaml.Node) {
			switch fieldName {
			case "code":
				celFound = h.parseCode(ctx, keyID, val, p) || celFound
			case "rego", "libs":
				// A legacy Rego-only target. Recorded so that a missing CEL
				// implementation can be reported with an actionable message.
				p.SetMetadata(MetadataHasRegoEngine, true)
			}
		})
	}
	return celFound
}

// parseCode walks the engines declared for a target and parses the source of
// the CEL engine. It reports whether CEL code was found.
func (h *templateTagHandler) parseCode(ctx policy.ParserContext, codeID int64, node *yaml.Node, p *policy.Policy) bool {
	if !assertList(ctx, codeID, "code", node) {
		return false
	}
	celFound := false
	for _, entry := range node.Content {
		var engine string
		var source *yaml.Node
		var sourceID int64
		rangeMap(ctx, entry, func(fieldName string, keyID int64, val *yaml.Node) {
			switch fieldName {
			case "engine":
				engine = val.Value
			case "source":
				source, sourceID = val, keyID
			}
		})
		switch engine {
		case EngineName:
			if source == nil {
				ctx.ReportErrorAtID(codeID, "%s engine is missing its source", EngineName)
				continue
			}
			if h.celFound {
				ctx.ReportErrorAtID(sourceID, "only one %s engine may be declared", EngineName)
				continue
			}
			// The engine counts as found even when its source turns out to
			// be unusable, so that the reason reported is the one which is
			// wrong with the template.
			h.celFound, celFound = true, true
			rule := ctx.ParseRule(ctx, p, source)
			if len(rule.Matches()) == 0 {
				ctx.ReportErrorAtID(sourceID, "%s source declares no validations", EngineName)
				continue
			}
			p.SetRule(h.ruleWithPrelude(ctx, sourceID, rule))
		case RegoEngineName:
			p.SetMetadata(MetadataHasRegoEngine, true)
		}
	}
	return celFound
}

// RuleTag handles the fields of a K8sNativeValidation source. The `variables`
// field is handled by the policy parser itself, as it shares the CEL policy
// syntax.
func (h *templateTagHandler) RuleTag(ctx policy.ParserContext, id int64, tagName string, node *yaml.Node, p *policy.Policy, r *policy.Rule) {
	switch tagName {
	case "validations":
		if !assertList(ctx, id, "validations", node) {
			return
		}
		for _, val := range node.Content {
			r.AddMatch(finalizeValidation(ctx, p, val))
		}
	case "generateVAP":
		p.SetMetadata(MetadataGenerateVAP, node.Value == "true")
	case "failurePolicy":
		p.SetMetadata(MetadataFailurePolicy, node.Value)
	case "matchConditions":
		// Match conditions gate whether the validations are evaluated at all.
		// They are parsed into a rule which nests the validations so that a
		// request the policy does not match produces no violation.
		h.parseMatchConditions(ctx, id, node, p)
	default:
		ctx.ReportErrorAtID(id, "unsupported %s source field: %s", EngineName, tagName)
	}
}

// parseMatchConditions collects the match conditions of a source. They are
// applied by ruleWithPrelude once the validations have been parsed.
func (h *templateTagHandler) parseMatchConditions(ctx policy.ParserContext, id int64, node *yaml.Node, p *policy.Policy) {
	if !assertList(ctx, id, "matchConditions", node) {
		return
	}
	for _, cond := range node.Content {
		var name, expr policy.ValueString
		rangeMap(ctx, cond, func(fieldName string, keyID int64, val *yaml.Node) {
			switch fieldName {
			case "name":
				name = ctx.NewString(val)
			case "expression":
				expr = ctx.NewString(val)
			default:
				ctx.ReportErrorAtID(keyID, "unsupported matchCondition field: %s", fieldName)
			}
		})
		if expr.Value == "" {
			ctx.ReportErrorAtID(id, "matchCondition %q is missing an expression", name.Value)
			continue
		}
		h.matchConditions = append(h.matchConditions, expr)
	}
}

// ruleWithPrelude wraps the rule holding a source's validations in the rule
// Gatekeeper places around every template: the variables it defines for all
// policies, and the match conditions which decide whether the validations apply
// at all. A request the policy does not match produces no violation.
//
// Each match condition becomes a variable, which keeps it type-checked and
// reported against its own position within the template.
func (h *templateTagHandler) ruleWithPrelude(ctx policy.ParserContext, sourceID int64, r *policy.Rule) *policy.Rule {
	prelude := policy.NewRule(sourceID)
	prelude.AddVariable(syntheticVariable(sourceID, ParamsVariable, paramsExpr))
	prelude.AddVariable(syntheticVariable(sourceID, AnyObjectVariable, anyObjectExpr))
	guards := make([]string, 0, len(h.matchConditions))
	for i, cond := range h.matchConditions {
		name := fmt.Sprintf("%s%d", matchConditionPrefix, i)
		v := policy.NewVariable(cond.ID)
		v.SetName(policy.ValueString{ID: cond.ID, Value: name})
		v.SetExpression(cond)
		prelude.AddVariable(v)
		guards = append(guards, variablePrefix+name)
	}
	condition := "true"
	if len(guards) != 0 {
		condition = strings.Join(guards, " && ")
	}
	m := policy.NewMatch(sourceID)
	m.SetCondition(policy.ValueString{ID: ctx.NextID(), Value: condition})
	m.SetRule(r)
	prelude.AddMatch(m)
	return prelude
}

// syntheticVariable declares a variable which Gatekeeper defines rather than
// the template author. It has no position of its own within the template, so it
// borrows the position of the source which introduced it.
func syntheticVariable(sourceID int64, name, expr string) *policy.Variable {
	v := policy.NewVariable(sourceID)
	v.SetName(policy.ValueString{ID: sourceID, Value: name})
	v.SetExpression(policy.ValueString{ID: sourceID, Value: expr})
	return v
}

// MatchTag handles the fields of a single validation entry.
func (h *templateTagHandler) MatchTag(ctx policy.ParserContext, id int64, tagName string, node *yaml.Node, p *policy.Policy, m *policy.Match) {
	if !m.HasOutput() || m.Output().Value == "" {
		m.SetOutput(policy.ValueString{ID: id, Value: defaultMessage})
	}
	switch tagName {
	case "expression":
		// A validation expression states what must be true, so a violation is
		// raised when it evaluates to false.
		cond := ctx.NewString(node)
		cond.Value = "!(" + cond.Value + ")"
		m.SetCondition(cond)
	case "message":
		// A static message: the output of a match is an expression, so the
		// message is turned into a CEL string literal.
		msg := ctx.NewString(node)
		msg.Value = strconv.Quote(msg.Value)
		m.SetOutput(msg)
	case "messageExpression":
		m.SetOutput(ctx.NewString(node))
	default:
		ctx.ReportErrorAtID(id, "unsupported validation field: %s", tagName)
	}
}

// finalizeValidation parses a validation and supplies the default message for
// validations which declare neither `message` nor `messageExpression`.
func finalizeValidation(ctx policy.ParserContext, p *policy.Policy, node *yaml.Node) *policy.Match {
	m := ctx.ParseMatch(ctx, p, node)
	if !strings.HasPrefix(m.Condition().Value, "!(") {
		ctx.ReportErrorAtID(m.SourceID(), "validation is missing an expression")
		return m
	}
	if m.Output().Value != defaultMessage {
		return m
	}
	out := m.Output()
	out.Value = strconv.Quote("failed expression: " + strings.TrimSuffix(strings.TrimPrefix(m.Condition().Value, "!("), ")"))
	m.SetOutput(out)
	return m
}

// rangeMap visits the key-value pairs of a YAML mapping node, reporting an
// error if the node is not a mapping.
func rangeMap(ctx policy.ParserContext, node *yaml.Node, visit func(fieldName string, keyID int64, val *yaml.Node)) {
	if node.Kind != yaml.MappingNode {
		ctx.ReportErrorAtID(ctx.CollectMetadata(node), "invalid value, expected a map got: %s", node.LongTag())
		return
	}
	for i := 0; i < len(node.Content)-1; i += 2 {
		key, val := node.Content[i], node.Content[i+1]
		// A folded or literal scalar reports the position of its key rather
		// than of its content, which the policy source positions are relative
		// to.
		if val.Style == yaml.FoldedStyle || val.Style == yaml.LiteralStyle {
			val.Line++
			val.Column = key.Column + 1
		}
		visit(key.Value, ctx.CollectMetadata(key), val)
	}
}

// assertList reports an error if the node is not a YAML list.
func assertList(ctx policy.ParserContext, id int64, fieldName string, node *yaml.Node) bool {
	if node.Kind != yaml.SequenceNode {
		ctx.ReportErrorAtID(id, "invalid '%s' type, expected list got: %s", fieldName, node.LongTag())
		return false
	}
	return true
}
