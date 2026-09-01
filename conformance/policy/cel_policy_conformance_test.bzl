# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Macro to run CEL policy conformance tests."""

load("@rules_shell//shell:sh_test.bzl", "sh_test")

def cel_policy_conformance_test_go(
        name,
        testdata,
        file_descriptor_set = None,
        test_cases = [],
        skip_tests = [],
        **kwargs):
    """Macro to run CEL policy conformance tests for Go.

    Args:
        name: The name of the test target.
        testdata: Testdata filegroup target.
        file_descriptor_set: (optional) File descriptor set target.
        test_cases: (optional) List of test case names (directory names) to run.
        skip_tests: (optional) List of test case names (directory names) to skip.
        **kwargs: Other standard Bazel target attributes.
    """

    lbl = native.package_relative_label(testdata)
    repo_prefix = lbl.workspace_name + "/" if lbl.workspace_name else ""
    testdata_dir = repo_prefix + lbl.package + "/" + lbl.name

    go_default_test_label = Label(":go_default_test")
    args = [
        "$(location %s)" % go_default_test_label,
        "--testdata_dir=" + testdata_dir,
    ]
    data = [go_default_test_label, testdata]

    if file_descriptor_set:
        args.append("--file_descriptor_set=$(rlocationpath " + str(file_descriptor_set) + ")")
        data.append(file_descriptor_set)

    if test_cases:
        args.append("--tests=" + ",".join(test_cases))
    if skip_tests:
        args.append("--skip_tests=" + ",".join(skip_tests))

    sh_test(
        name = name,
        size = "small",
        srcs = ["conformance_test.sh"],
        args = args,
        data = data,
        **kwargs
    )
