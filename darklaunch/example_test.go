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

package darklaunch_test

import (
	"fmt"
	"log"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/darklaunch"
)

// Example dark-launches a rewritten authorization policy against a control,
// using named probes to locate where the two disagree.
//
// Both expressions return false for this input, so a comparison of the final
// results alone would report agreement. The probes show the agreement is
// accidental: the candidate reaches the same answer through a different
// subexpression.
func Example() {
	env, err := cel.NewEnv(
		darklaunch.ProbeDecls(),
		cel.Variable("role", cel.StringType),
		cel.Variable("tenant", cel.StringType),
		cel.Variable("verified", cel.BoolType),
	)
	if err != nil {
		log.Fatalf("cel.NewEnv() failed: %v", err)
	}

	const (
		control   = `trace('is_admin', role == 'admin') && trace('is_verified', verified)`
		candidate = `trace('is_admin', role in ['admin', 'owner']) && trace('is_verified', verified && tenant != '')`
	)

	input := map[string]any{"role": "owner", "tenant": "", "verified": true}
	agg := darklaunch.NewAggregate()

	for _, c := range []struct {
		name string
		expr string
		role string
	}{
		{"control", control, darklaunch.RoleShadowControl},
		{"candidate", candidate, darklaunch.RoleCandidate},
	} {
		a, iss := env.Compile(c.expr)
		if iss.Err() != nil {
			log.Fatalf("Compile() failed: %v", iss.Err())
		}
		pool, err := darklaunch.NewPool(env, a)
		if err != nil {
			log.Fatalf("NewPool() failed: %v", err)
		}
		res, err := pool.Eval(input)
		if err != nil {
			log.Fatalf("Eval() failed: %v", err)
		}
		// Dark-launched results are never returned to a caller, so Applied is
		// false for both: the control here is the instrumented shadow copy.
		agg.Record(darklaunch.Attrs{
			FlagKey:     "tenant.admin_access",
			Role:        c.role,
			Applied:     false,
			Environment: "production",
		}, res)

		fmt.Printf("%s -> %v\n", c.name, res.Value)
		for _, p := range res.Probes {
			fmt.Printf("  %-12s %-6s %v\n", p.Name, p.Kind, p.Value)
		}
	}

	fmt.Println("metrics:")
	for _, s := range agg.Snapshot() {
		if s.Name != darklaunch.MetricProbeObservations {
			continue
		}
		fmt.Printf("  %s{%s=%s,%s=%s,%s=%s} %d\n",
			s.Name,
			darklaunch.AttrRole, s.Attributes[darklaunch.AttrRole],
			darklaunch.AttrProbeName, s.Attributes[darklaunch.AttrProbeName],
			darklaunch.AttrResultKind, s.Attributes[darklaunch.AttrResultKind],
			s.Count)
	}

	// Output:
	// control -> false
	//   is_admin     value  false
	// candidate -> false
	//   is_admin     value  true
	//   is_verified  value  false
	// metrics:
	//   cel.probe.observations{cel.evaluation.role=candidate,cel.probe.name=is_admin,cel.result.kind=value} 1
	//   cel.probe.observations{cel.evaluation.role=candidate,cel.probe.name=is_verified,cel.result.kind=value} 1
	//   cel.probe.observations{cel.evaluation.role=shadow_control,cel.probe.name=is_admin,cel.result.kind=value} 1
}
