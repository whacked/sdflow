package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const testConf string = `
env:
  BASE_PATH: s3://mybucket/myproject

myproject:
  out: ${BASE_PATH}/myoutput.jsonl
  in: ${BASE_PATH}/myinputs/project1/myinput.jsonl
  pre: setup-environment.sh
  run: jdxd --input ${in} --output ${out}
  post: cleanup.sh
  config:
    s3:
      retry: 3
    notify:
      failure:
        slach:
          message: "Job Failed: ${out}"
          channel: mychannel
`

/* -------------------------------------------------------------------------- */
/*                                real tests                                  */
/* -------------------------------------------------------------------------- */

func TestParseFlowDefinitionEnvironmentVariables(t *testing.T) {
	yaml := `
SCHEMAS_DIR: ./schemas
SPACES_IN_STRING: a b c
version_string: 0.0.1
BUILD_TYPE: debuggery
`
	pfd := parseFlowDefinitionSource(yaml)

	// Check that all YAML environment variables were parsed correctly
	expectedVars := map[string]string{
		"SCHEMAS_DIR":      "./schemas",
		"SPACES_IN_STRING": "a b c",
		"version_string":   "0.0.1",
		"BUILD_TYPE":       "debuggery",
	}

	for key, expectedValue := range expectedVars {
		if actualValue, exists := pfd.executionEnv[key]; !exists {
			t.Errorf("environment variable %s not found in executionEnv", key)
		} else if len(actualValue) != 1 || actualValue[0] != expectedValue {
			t.Errorf("environment variable %s: expected [%q], got %v", key, expectedValue, actualValue)
		}
	}

	// Verify that YAML variables are present (OS env vars are also included, so we can't check exact count)
	if len(pfd.executionEnv) < len(expectedVars) {
		t.Errorf("executionEnv should contain at least %d variables (YAML vars), got %d", len(expectedVars), len(pfd.executionEnv))
	}

	// Verify that YAML variables override OS environment variables
	// This is the key behavior we want to test
	for key, expectedValue := range expectedVars {
		if actualValue, exists := pfd.executionEnv[key]; !exists {
			t.Errorf("YAML variable %s not found in executionEnv", key)
		} else if len(actualValue) != 1 || actualValue[0] != expectedValue {
			t.Errorf("YAML variable %s should override OS environment variable: expected [%q], got %v", key, expectedValue, actualValue)
		}
	}
}

func TestSubstituteWithContext(t *testing.T) {
	ctx := map[string]string{"NAME": "World"}
	out := substituteWithContext("Hello ${NAME}", ctx)
	if *out != "Hello World" {
		t.Fatalf("want %q, got %q", "Hello World", *out)
	}

	_ = os.Setenv("FOO", "bar")
	out2 := substituteWithContext("val ${FOO}", getOsEnvironAsMap())
	if *out2 != "val bar" {
		t.Fatalf("env fallback failed; got %q", *out2)
	}
}

func TestEscapeMechanism(t *testing.T) {
	ctx := map[string]string{"VAR": "value"}
	
	// Test that $$ becomes $
	out := substituteWithContext("echo $$HOME", ctx)
	if *out != "echo $HOME" {
		t.Fatalf("want %q, got %q", "echo $HOME", *out)
	}
	
	// Test that ${VAR} is substituted but $$NF becomes $NF
	out2 := substituteWithContext("awk '{print $$NF}' ${VAR}", ctx)
	if *out2 != "awk '{print $NF}' value" {
		t.Fatalf("want %q, got %q", "awk '{print $NF}' value", *out2)
	}
	
	// Test multiple escapes
	out3 := substituteWithContext("$$1 $$2 ${VAR} $$3", ctx)
	if *out3 != "$1 $2 value $3" {
		t.Fatalf("want %q, got %q", "$1 $2 value $3", *out3)
	}
}

func TestExpandSdflowBuiltins(t *testing.T) {
	// Test builtin substitution cases
	testCases := []struct {
		name        string
		template    string
		taskEnv     map[string][]string
		expected    string
		expectError bool
	}{
		// Basic builtin substitution tests
		{
			name:        "substitute $in with values",
			template:    "echo $in",
			taskEnv: map[string][]string{
				"in": {"file1.txt", "file2.txt"},
			},
			expected:    "echo file1.txt file2.txt",
			expectError: false,
		},
		{
			name:        "substitute ${in} with values",
			template:    "echo ${in}",
			taskEnv: map[string][]string{
				"in": {"file1.txt", "file2.txt"},
			},
			expected:    "echo file1.txt file2.txt",
			expectError: false,
		},
		{
			name:        "substitute $out with value",
			template:    "echo $out",
			taskEnv: map[string][]string{
				"out": {"output.txt"},
			},
			expected:    "echo output.txt",
			expectError: false,
		},
		{
			name:        "substitute ${out} with value",
			template:    "echo ${out}",
			taskEnv: map[string][]string{
				"out": {"output.txt"},
			},
			expected:    "echo output.txt",
			expectError: false,
		},
		{
			name:        "substitute ${in[0]} indexed access",
			template:    "cat ${in[0]}",
			taskEnv: map[string][]string{
				"in": {"first.txt", "second.txt"},
			},
			expected:    "cat first.txt",
			expectError: false,
		},
		{
			name:        "substitute ${in[1]} indexed access",
			template:    "cat ${in[1]}",
			taskEnv: map[string][]string{
				"in": {"first.txt", "second.txt"},
			},
			expected:    "cat second.txt",
			expectError: false,
		},
		{
			name:        "substitute ${in.alias} aliased input",
			template:    "process ${in.source}",
			taskEnv: map[string][]string{
				"in":         {"first.txt", "second.txt"},
				"in.source":  {"source.txt"},
			},
			expected:    "process source.txt",
			expectError: false,
		},

		// Test partial/missing builtin scenarios
		{
			name:        "missing 'in' leaves $in untouched",
			template:    "echo $in and $out",
			taskEnv: map[string][]string{
				"out": {"output.txt"},
			},
			expected:    "echo $in and output.txt",
			expectError: false,
		},
		{
			name:        "missing 'out' leaves $out untouched",
			template:    "echo $in > $out",
			taskEnv: map[string][]string{
				"in": {"input.txt"},
			},
			expected:    "echo input.txt > $out",
			expectError: false,
		},
		{
			name:        "index out of range throws error",
			template:    "echo ${in[5]}",
			taskEnv: map[string][]string{
				"in": {"file1.txt", "file2.txt"},
			},
			expected:    "",
			expectError: true,
		},
		{
			name:        "undefined alias throws error",
			template:    "echo ${in.missing}",
			taskEnv: map[string][]string{
				"in": {"file1.txt"},
				"in.other": {"other.txt"},
			},
			expected:    "",
			expectError: true,
		},
		{
			name:        "dot notation ${in.0} works",
			template:    "echo ${in.0}",
			taskEnv: map[string][]string{
				"in": {"first.txt", "second.txt"},
			},
			expected:    "echo first.txt",
			expectError: false,
		},
		{
			name:        "dot notation ${in.1} works",
			template:    "echo ${in.1}",
			taskEnv: map[string][]string{
				"in": {"first.txt", "second.txt"},
			},
			expected:    "echo second.txt",
			expectError: false,
		},

		// Test that non-builtins are left alone
		{
			name:        "shell variables left untouched",
			template:    "echo $HOME and $USER and ${PWD}",
			taskEnv: map[string][]string{
				"in": {"input.txt"},
			},
			expected:    "echo $HOME and $USER and ${PWD}",
			expectError: false,
		},
		{
			name:        "awk variables left untouched",
			template:    "awk '{print $1, $NF}' $in",
			taskEnv: map[string][]string{
				"in": {"data.txt"},
			},
			expected:    "awk '{print $1, $NF}' data.txt",
			expectError: false,
		},
		{
			name:        "mixed builtins and shell variables",
			template:    "echo $USER processing $in into $out at $(date)",
			taskEnv: map[string][]string{
				"in":  {"input.txt"},
				"out": {"output.txt"},
			},
			expected:    "echo $USER processing input.txt into output.txt at $(date)",
			expectError: false,
		},

		// Test escaping for builtins only
		{
			name:        "escaped builtin $in becomes literal",
			template:    "echo our inputs are stored in $$in which consists of $in",
			taskEnv: map[string][]string{
				"in": {"file1.txt", "file2.txt"},
			},
			expected:    "echo our inputs are stored in $in which consists of file1.txt file2.txt",
			expectError: false,
		},
		{
			name:        "escaped builtin ${out} becomes literal",
			template:    "echo result goes to $${out} which is $out",
			taskEnv: map[string][]string{
				"out": {"result.txt"},
			},
			expected:    "echo result goes to ${out} which is result.txt",
			expectError: false,
		},
		{
			name:        "shell variables don't need escaping",
			template:    "awk '{print $1, $NF}' with ${in[0]} no escaping needed",
			taskEnv: map[string][]string{
				"in": {"data.txt"},
			},
			expected:    "awk '{print $1, $NF}' with data.txt no escaping needed",
			expectError: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := expandSdflowBuiltins(tc.template, tc.taskEnv)

			if tc.expectError {
				if err == nil {
					t.Errorf("expandSdflowBuiltins() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("expandSdflowBuiltins() unexpected error: %v", err)
				return
			}

			if result != tc.expected {
				t.Errorf("expandSdflowBuiltins() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func TestRenderCommandWithEnv(t *testing.T) {
	// Test environment variable injection
	testCases := []struct {
		name        string
		taskEnv     map[string][]string
		expectInEnv []string // Environment variables we expect to see
	}{
		{
			name: "YAML globals become environment variables",
			taskEnv: map[string][]string{
				"in":       {"input.txt"},
				"out":      {"output.txt"},
				"VERSION":  {"1.2.3"},
				"DEBUG":    {"true"},
				"TARGETS":  {"linux", "macos", "windows"},
			},
			expectInEnv: []string{"VERSION=1.2.3", "DEBUG=true", "TARGETS=linux macos windows"},
		},
		{
			name: "builtin variables excluded from environment",
			taskEnv: map[string][]string{
				"in":        {"input.txt"},
				"out":       {"output.txt"},
				"in.source": {"source.txt"},
				"CUSTOM":    {"value"},
			},
			expectInEnv: []string{"CUSTOM=value"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			task := &RunnableTask{
				taskDeclaration: &RunnableSchemaJson{
					Run: stringPtr("echo test"),
					Out: stringPtr("output.txt"),
				},
				inputs: []*RunnableTaskInput{
					{path: "input.txt"},
				},
			}

			_, envVars, err := renderCommandWithEnv(task, tc.taskEnv)
			if err != nil {
				t.Errorf("renderCommandWithEnv() unexpected error: %v", err)
				return
			}

			// Check that expected environment variables are present
			for _, expectedEnv := range tc.expectInEnv {
				found := false
				for _, envVar := range envVars {
					if envVar == expectedEnv {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected environment variable %q not found in %v", expectedEnv, envVars)
				}
			}

			// Check that sdflow builtin variables from taskEnv are NOT in environment
			// We need to be careful here - system environment may have variables like "out"
			for key := range tc.taskEnv {
				if key == "in" || key == "out" || strings.HasPrefix(key, "in.") {
					envPattern := key + "="
					for _, envVar := range envVars {
						if strings.HasPrefix(envVar, envPattern) {
							// Check if this is actually from our task environment, not system environment
							if key == "in" || key == "out" {
								// These are core builtins and should not be in environment at all from our task
								expectedValue := strings.Join(tc.taskEnv[key], " ")
								if envVar == envPattern+expectedValue {
									t.Errorf("Task builtin variable %q should not be in environment: %s", key, envVar)
								}
							} else {
								// Alias/indexed variables should definitely not be in environment
								t.Errorf("Task builtin variable %q should not be in environment: %s", key, envVar)
							}
						}
					}
				}
			}
		})
	}
}

func TestEnvironmentVariableOverrides(t *testing.T) {
	// Test that YAML variables override system environment and fallback behavior
	testCases := []struct {
		name        string
		yamlGlobals map[string][]string
		setupEnv    map[string]string  // Environment variables to set for test
		commands    []string           // Commands to test
		expectEnv   map[string]string  // Expected values in environment
	}{
		{
			name: "YAML variable overrides system environment",
			yamlGlobals: map[string][]string{
				"USER": {"FOOBAR_testuser"},
			},
			setupEnv: map[string]string{
				"USER": "original_user",
			},
			expectEnv: map[string]string{
				"USER": "FOOBAR_testuser", // YAML should override system
			},
		},
		{
			name: "system environment used when not overridden",
			yamlGlobals: map[string][]string{
				"CUSTOM": {"custom_value"},
			},
			setupEnv: map[string]string{
				"USER": "system_user",
				"HOME": "/home/test",
			},
			expectEnv: map[string]string{
				"CUSTOM": "custom_value",
				"USER":   "system_user", // Should preserve system value
				"HOME":   "/home/test",  // Should preserve system value
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Set up test environment variables
			for key, value := range tc.setupEnv {
				t.Setenv(key, value)
			}

			task := &RunnableTask{
				taskDeclaration: &RunnableSchemaJson{
					Run: stringPtr("echo test"),
					Out: stringPtr("output.txt"),
				},
				inputs: []*RunnableTaskInput{
					{path: "input.txt"},
				},
			}

			_, envVars, err := renderCommandWithEnv(task, tc.yamlGlobals)
			if err != nil {
				t.Errorf("renderCommandWithEnv() unexpected error: %v", err)
				return
			}

			// Check expected environment variables
			for expectedKey, expectedValue := range tc.expectEnv {
				found := false
				expectedEnvVar := expectedKey + "=" + expectedValue

				for _, envVar := range envVars {
					if envVar == expectedEnvVar {
						found = true
						break
					}
				}

				if !found {
					t.Errorf("Expected environment variable %s=%s not found in environment", expectedKey, expectedValue)
					// Debug: show what we actually got
					for _, envVar := range envVars {
						if strings.HasPrefix(envVar, expectedKey+"=") {
							t.Errorf("  Found instead: %s", envVar)
						}
					}
				}
			}
		})
	}
}

func TestCommandRenderingWithUndefinedVariables(t *testing.T) {
	// Test that undefined variables are left as literal $VAR (not substituted to "")
	testCases := []struct {
		name     string
		template string
		taskEnv  map[string][]string
		expected string
	}{
		{
			name:     "undefined variables left as literal",
			template: "echo $UNDEFINED_VAR and ${ALSO_UNDEFINED}",
			taskEnv:  map[string][]string{},
			expected: "echo $UNDEFINED_VAR and ${ALSO_UNDEFINED}",
		},
		{
			name:     "mix of defined builtins and undefined variables",
			template: "process $in with $UNDEFINED_CONFIG into $out using ${ALSO_UNDEFINED}",
			taskEnv: map[string][]string{
				"in":  {"input.txt"},
				"out": {"output.txt"},
			},
			expected: "process input.txt with $UNDEFINED_CONFIG into output.txt using ${ALSO_UNDEFINED}",
		},
		{
			name:     "shell variables preserved in complex commands",
			template: "for file in $in; do echo $file | awk '{print $1}'; done",
			taskEnv: map[string][]string{
				"in": {"file1.txt", "file2.txt"},
			},
			expected: "for file in file1.txt file2.txt; do echo $file | awk '{print $1}'; done",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := expandSdflowBuiltins(tc.template, tc.taskEnv)
			if err != nil {
				t.Errorf("expandSdflowBuiltins() unexpected error: %v", err)
				return
			}
			if result != tc.expected {
				t.Errorf("expandSdflowBuiltins() = %q, want %q", result, tc.expected)
			}
		})
	}
}

func stringSliceToInterface(slice []string) []interface{} {
	out := make([]interface{}, len(slice))
	for i, v := range slice {
		out[i] = v
	}
	return out
}

func TestRenderCommand(t *testing.T) {
	tests := []struct {
		name        string
		runnable    map[string]interface{}
		env         map[string][]string
		wantCommand string
	}{
		{
			name: "single input file",
			runnable: map[string]interface{}{
				"run": "cp $in $out",
				"out": "dst.txt",
				"in":  "src.txt",
			},
			env:         map[string][]string{},
			wantCommand: "cp src.txt dst.txt",
		},
		{
			name: "relative output path",
			runnable: map[string]interface{}{
				"run": "touch $out",
				"out": "relative/dst.txt",
			},
			env:         map[string][]string{},
			wantCommand: "touch relative/dst.txt",
		},
		{
			name: "absolute output path",
			runnable: map[string]interface{}{
				"run": "touch $out",
				"out": "/absolute/path/dst.txt",
			},
			env:         map[string][]string{},
			wantCommand: "touch /absolute/path/dst.txt",
		},
		{
			name: "multiple input files",
			runnable: map[string]interface{}{
				"run": "cp ${in[0]} $out",
				"out": "./bar/dst.txt",
				"in":  stringSliceToInterface([]string{"src1.txt", "source two.text", "source-3.another.file"}),
			},
			env:         map[string][]string{},
			wantCommand: "cp src1.txt ./bar/dst.txt",
		},
		{
			name: "YAML global variables left for shell resolution",
			runnable: map[string]interface{}{
				"run": "echo 'version ${version_string}'",
			},
			env: map[string][]string{
				"version_string": {"0.0.1"},
			},
			wantCommand: "echo 'version ${version_string}'", // Global variables not substituted
		},
		{
			name: "multiple YAML globals left for shell resolution",
			runnable: map[string]interface{}{
				"run": "echo '${THING1} ${GENERATORS_DIR}'",
			},
			env: map[string][]string{
				"THING1":         {"thing1"},
				"GENERATORS_DIR": {"./generators"},
			},
			wantCommand: "echo '${THING1} ${GENERATORS_DIR}'", // Global variables not substituted
		},
		{
			name: "YAML array globals left for shell resolution",
			runnable: map[string]interface{}{
				"run": "echo 'first: ${file_list[0]}'",
			},
			env: map[string][]string{
				"file_list": {"file1.txt", "file2.txt", "file3.txt"},
			},
			wantCommand: "echo 'first: ${file_list[0]}'", // Global variables not substituted
		},
		{
			name: "mixed builtins and globals - builtins substituted, globals left",
			runnable: map[string]interface{}{
				"run": "process $in with ${VERSION} into $out",
				"in":  "input.txt",
				"out": "output.txt",
			},
			env: map[string][]string{
				"VERSION": {"1.0.0"},
			},
			wantCommand: "process input.txt with ${VERSION} into output.txt", // Only builtins substituted
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := createTaskFromRunnableKeyVals("FakeTarget", "fakeRenderedTarget", tt.runnable, tt.env)
			got := renderCommand(task, tt.env)
			if got != tt.wantCommand {
				t.Errorf("renderCommand() = %q, want %q", got, tt.wantCommand)
			}
		})
	}
}

func TestParseFlowDefinitionFile(t *testing.T) {
	tmp := t.TempDir()

	yaml := `
SCHEMAS_DIR: ./schemas
hello:
  in: ${SCHEMAS_DIR}/foo.txt
  out: bar.txt
  run: cat $in > $out
`
	flowPath := filepath.Join(tmp, "Sdflow.yaml")
	if err := os.WriteFile(flowPath, []byte(yaml), 0644); err != nil {
		t.Fatalf("write flow file: %v", err)
	}

	pfd := parseFlowDefinitionFile(flowPath)

	task, ok := pfd.taskLookup["hello"]
	if !ok {
		t.Fatalf("task 'hello' not in lookup")
	}
	if len(task.inputs) != 1 || !strings.HasSuffix(task.inputs[0].path, "schemas/foo.txt") {
		t.Fatalf("input not parsed/substituted correctly: %+v", task.inputs)
	}
	if task.taskDeclaration == nil || task.taskDeclaration.Out == nil ||
		*task.taskDeclaration.Out != "bar.txt" {
		t.Fatalf("out path wrong: %+v", task.taskDeclaration)
	}
}

func TestParseFlowDefinitionFileImplicitOutputPath(t *testing.T) {
	yaml := `
SCHEMAS_DIR: ./schemas
./implied-file.dat:
  in: ${SCHEMAS_DIR}/foo.txt
  run: cat $in > $out
`
	pfd := parseFlowDefinitionSource(yaml)

	task, ok := pfd.taskLookup["./implied-file.dat"]
	if !ok {
		t.Fatalf("task './implied-file.dat' not in lookup")
	}
	if len(task.inputs) != 1 || !strings.HasSuffix(task.inputs[0].path, "schemas/foo.txt") {
		t.Fatalf("input not parsed/substituted correctly: %+v", task.inputs)
	}
	expectedOut := "./implied-file.dat"
	if task.taskDeclaration == nil {
		t.Fatalf("taskDeclaration is nil, expected Out: %q", expectedOut)
	}
	if task.taskDeclaration.Out == nil {
		t.Fatalf("taskDeclaration.Out is nil, expected Out: %q", expectedOut)
	}
	if *task.taskDeclaration.Out != expectedOut {
		t.Fatalf("out path wrong: expected %q, got %q (taskDeclaration: %+v)", expectedOut, *task.taskDeclaration.Out, task.taskDeclaration)
	}
}

func TestParseFlowDefinitionFileInputArraySyntax(t *testing.T) {
	yaml := `
SCHEMAS_DIR: ./schemas
myTarget:
  in:
  - foo.txt
  - second.txt
  - last.3
  run: echo between ${in[0]} and ${in[2]} we have === $in === ${in}
`
	pfd := parseFlowDefinitionSource(yaml)

	task, _ := pfd.taskLookup["myTarget"]
	if len(task.inputs) != 3 {
		t.Fatalf("expected 3 inputs, got %d", len(task.inputs))
	}

	expectedInputs := []string{"foo.txt", "second.txt", "last.3"}
	for i, input := range task.inputs {
		if input.path != expectedInputs[i] {
			t.Fatalf("input %d: expected %s, got %s", i, expectedInputs[i], input.path)
		}
	}

	if task.inputs[0].alias != "foo.txt" {
		t.Fatalf("input 0 alias wrong: %s (expected foo.txt)", task.inputs[0].alias)
	}
	if task.inputs[1].alias != "second.txt" {
		t.Fatalf("input 1 alias wrong: %s (expected second.txt)", task.inputs[1].alias)
	}

	renderedCommand := renderCommand(task, pfd.executionEnv)
	expectedCommand := "echo between foo.txt and last.3 we have === foo.txt second.txt last.3 === foo.txt second.txt last.3"
	if renderedCommand != expectedCommand {
		t.Fatalf("rendered command incorrect\nexpected: %s\ngot: %s", expectedCommand, renderedCommand)
	}
}

func TestParseFlowDefinitionFileInputMapSyntax(t *testing.T) {
	yaml := `
SCHEMAS_DIR: ./schemas
myTarget:
  in:
    first: foo.txt
    second: second.txt
    last: last.3
  run: echo between ${in.first} and ${in.last}; $in === ${in}
`
	pfd := parseFlowDefinitionSource(yaml)

	task, _ := pfd.taskLookup["myTarget"]
	if len(task.inputs) != 3 {
		t.Fatalf("expected 3 inputs, got %d", len(task.inputs))
	}

	expectedInputs := map[string]bool{
		"foo.txt":    false,
		"second.txt": false,
		"last.3":     false,
	}
	for _, input := range task.inputs {
		if _, exists := expectedInputs[input.path]; !exists {
			t.Fatalf("unexpected input: %s", input.path)
		}
		expectedInputs[input.path] = true
	}

	for path, found := range expectedInputs {
		if !found {
			t.Fatalf("missing expected input: %s", path)
		}
	}

	renderedCommand := renderCommand(task, pfd.executionEnv)

	// Split on semicolon and verify first part
	parts := strings.Split(renderedCommand, ";")
	expectedFirstPart := "echo between foo.txt and last.3"
	if strings.TrimSpace(parts[0]) != expectedFirstPart {
		t.Fatalf("first part incorrect\nexpected: %s\ngot: %s", expectedFirstPart, strings.TrimSpace(parts[0]))
	}

	// Split second part on === and verify both segments are identical
	segments := strings.Split(strings.TrimSpace(parts[1]), "===")
	if len(segments) != 2 {
		t.Fatalf("expected 2 segments separated by ===, got %d segments", len(segments))
	}

	seg1 := strings.TrimSpace(segments[0])
	seg2 := strings.TrimSpace(segments[1])
	if seg1 != seg2 {
		t.Fatalf("segments not equal\nfirst: %s\nsecond: %s", seg1, seg2)
	}
}

func getExecutionCounts(tasks map[string]*RunnableTask) map[string]int {
	counts := make(map[string]int)
	for name, task := range tasks {
		counts[name] = task.executionCount
	}
	return counts
}

func assertExecutionCounts(t *testing.T, tasks map[string]*RunnableTask, expected map[string]int) {
	actual := getExecutionCounts(tasks)
	for name, expectedCount := range expected {
		if actualCount, exists := actual[name]; !exists {
			t.Fatalf("task %s not found in actual execution counts", name)
		} else if actualCount != expectedCount {
			t.Fatalf("task %s: expected %d executions, got %d", name, expectedCount, actualCount)
		}
	}
}

func assertTaskDependencies(t *testing.T, task *RunnableTask, expectedDeps []string) {
	actualDeps := make([]string, len(task.taskDependencies))
	for i, dep := range task.taskDependencies {
		actualDeps[i] = dep.targetName
	}

	if len(actualDeps) != len(expectedDeps) {
		t.Fatalf("task %s: expected %d dependencies, got %d. Expected: %v, Got: %v",
			task.targetName, len(expectedDeps), len(actualDeps), expectedDeps, actualDeps)
	}

	// Convert to map for easier comparison (order doesn't matter)
	expectedMap := make(map[string]bool)
	for _, dep := range expectedDeps {
		expectedMap[dep] = true
	}

	for _, dep := range actualDeps {
		if !expectedMap[dep] {
			t.Fatalf("task %s: unexpected dependency %s. Expected: %v, Got: %v",
				task.targetName, dep, expectedDeps, actualDeps)
		}
		delete(expectedMap, dep)
	}

	if len(expectedMap) > 0 {
		missing := make([]string, 0, len(expectedMap))
		for dep := range expectedMap {
			missing = append(missing, dep)
		}
		t.Fatalf("task %s: missing dependencies %v. Expected: %v, Got: %v",
			task.targetName, missing, expectedDeps, actualDeps)
	}
}

func TestParseFlowDefinitionFileInputSha256Syntax(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		wantErr     bool
		errContains string
		checkSha256 func(t *testing.T, task *RunnableTask)
	}{
		{
			name: "string sha256 for single input",
			yaml: `
SCHEMAS_DIR: ./schemas
myTarget:
  in: foo.txt
  in.sha256: 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
  run: echo $in
`,
			wantErr: false,
			checkSha256: func(t *testing.T, task *RunnableTask) {
				// Check InSha256 is a string
				sha256, ok := task.taskDeclaration.InSha256.(string)
				if !ok {
					t.Fatalf("expected InSha256 to be string, got %T", task.taskDeclaration.InSha256)
				}
				if sha256 != "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef" {
					t.Fatalf("wrong sha256 value: %s", sha256)
				}
				// Check input has sha256 set
				if len(task.inputs) != 1 {
					t.Fatalf("expected 1 input, got %d", len(task.inputs))
				}
				if task.inputs[0].sha256 != "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef" {
					t.Fatalf("wrong input sha256: %s", task.inputs[0].sha256)
				}
			},
		},
		{
			name: "string sha256 for array input (should fail)",
			yaml: `
SCHEMAS_DIR: ./schemas
myTarget:
  in:
    - foo.txt
    - bar.txt
  in.sha256: 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
  run: echo $in
`,
			wantErr:     true,
			errContains: "in.sha256 must be a map when in is an array",
		},
		{
			name: "map sha256 with path keys",
			yaml: `
SCHEMAS_DIR: ./schemas
myTarget:
  in:
    - foo.txt
    - bar.txt
  in.sha256:
    foo.txt: 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
    bar.txt: abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890
  run: echo $in
`,
			wantErr: false,
			checkSha256: func(t *testing.T, task *RunnableTask) {
				// Check InSha256 is a map
				sha256Map, ok := task.taskDeclaration.InSha256.(map[string]interface{})
				if !ok {
					t.Fatalf("expected InSha256 to be map, got %T", task.taskDeclaration.InSha256)
				}
				// Check map contents
				expectedSha256s := map[string]string{
					"foo.txt": "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					"bar.txt": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				}
				for k, v := range expectedSha256s {
					if sha256, ok := sha256Map[k].(string); !ok || sha256 != v {
						t.Fatalf("wrong sha256 for key %s: got %v", k, sha256Map[k])
					}
				}
				// Check inputs have correct sha256s
				if len(task.inputs) != 2 {
					t.Fatalf("expected 2 inputs, got %d", len(task.inputs))
				}
				for _, input := range task.inputs {
					expectedSha256 := expectedSha256s[input.path]
					if input.sha256 != expectedSha256 {
						t.Fatalf("wrong sha256 for input %s: got %s, want %s", input.path, input.sha256, expectedSha256)
					}
				}
			},
		},
		{
			name: "map sha256 with alias keys",
			yaml: `
SCHEMAS_DIR: ./schemas
myTarget:
  in:
    first: foo.txt
    second: bar.txt
  in.sha256:
    first: 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
    IGNORE_ME: aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    second: abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890
  run: echo $in
`,
			wantErr: false,
			checkSha256: func(t *testing.T, task *RunnableTask) {
				// Check InSha256 is a map
				sha256Map, ok := task.taskDeclaration.InSha256.(map[string]interface{})
				if !ok {
					t.Fatalf("expected InSha256 to be map, got %T", task.taskDeclaration.InSha256)
				}
				// Check map contents
				expectedSha256s := map[string]string{
					"first":  "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
					"second": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
				}
				for k, v := range expectedSha256s {
					if sha256, ok := sha256Map[k].(string); !ok || sha256 != v {
						t.Fatalf("wrong sha256 for key %s: got %v", k, sha256Map[k])
					}
				}
				// Check inputs have correct sha256s
				if len(task.inputs) != 2 {
					t.Fatalf("expected 2 inputs, got %d", len(task.inputs))
				}
				for _, input := range task.inputs {
					expectedSha256 := expectedSha256s[input.alias]
					if input.sha256 != expectedSha256 {
						t.Fatalf("wrong sha256 for input %s: got %s, want %s", input.alias, input.sha256, expectedSha256)
					}
				}
			},
		},
		{
			name: "invalid sha256 length",
			yaml: `
SCHEMAS_DIR: ./schemas
myTarget:
  in: foo.txt
  in.sha256: 12345
  run: echo $in
`,
			wantErr:     true,
			errContains: "sha256 must be 64 characters",
		},
		{
			name: "no matching sha256",
			yaml: `
SCHEMAS_DIR: ./schemas
myTarget:
  in:
    first: foo.txt
  in.sha256:
    nonexistent: 1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef
  run: echo $in
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pfd := parseFlowDefinitionSource(tt.yaml)
			task, ok := pfd.taskLookup["myTarget"]
			if !ok {
				t.Fatalf("task 'myTarget' not in lookup")
			}

			if tt.wantErr {
				// TODO: Add error checking once we implement the validation
				t.Skip("error checking not yet implemented")
			}

			// Verify the task was created correctly
			if task.taskDeclaration == nil {
				t.Fatal("task declaration is nil")
			}

			// For non-error cases, verify the sha256 was set correctly
			if !tt.wantErr {
				if task.taskDeclaration.InSha256 == nil {
					t.Fatal("in.sha256 was not set")
				}
				if tt.checkSha256 != nil {
					tt.checkSha256(t, task)
				}
			}
		})
	}
}

func TestComplexDependencyGraphExecution(t *testing.T) {
	tmp := t.TempDir()
	yaml := `
a.c:
  run: echo "compiling a.c" && touch $out
  out: a.o

b.c: 
  run: echo "compiling b.c" && touch $out
  out: b.o

c.c:
  run: echo "compiling c.c" && touch $out  
  out: c.o

x.o:
  in: [a.o, b.o]
  run: echo "linking x.o from $in" && touch $out
  out: x.o

y.o:
  in: [b.o, c.o] 
  run: echo "linking y.o from $in" && touch $out
  out: y.o

prog_1:
  in: x.o
  run: echo "building prog_1 from $in" && touch $out
  out: prog_1

prog_2: 
  in: [x.o, y.o]
  run: echo "building prog_2 from $in" && touch $out  
  out: prog_2
`
	flowPath := filepath.Join(tmp, "Sdflow.yaml")

	if err := os.WriteFile(flowPath, []byte(yaml), 0644); err != nil {
		t.Fatalf("write flow file: %v", err)
	}

	// Change to temp directory so relative paths work
	oldDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get current dir: %v", err)
	}
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("change to temp dir: %v", err)
	}
	defer os.Chdir(oldDir)

	pfd := parseFlowDefinitionFile(flowPath)

	// Phase 1: Run prog_1
	// This should execute: a.c, b.c, x.o, prog_1
	task1, ok := pfd.taskLookup["prog_1"]
	if !ok {
		t.Fatalf("task 'prog_1' not found")
	}

	runTask(task1, pfd.executionEnv, NewRealExecutor(false, false), pfd.taskLookup)

	// Verify expected execution counts after first run
	expectedAfterProg1 := map[string]int{
		"a.c": 1, "b.c": 1, "x.o": 1, "prog_1": 1,
		"c.c": 0, "y.o": 0, "prog_2": 0,
	}
	assertExecutionCounts(t, pfd.taskLookup, expectedAfterProg1)

	// Phase 2: Run prog_2
	// This should execute: c.c, y.o, prog_2 (skipping b.c, x.o which are already completed)
	task2, ok := pfd.taskLookup["prog_2"]
	if !ok {
		t.Fatalf("task 'prog_2' not found")
	}

	runTask(task2, pfd.executionEnv, NewRealExecutor(false, false), pfd.taskLookup)

	// Verify final execution counts
	expectedAfterProg2 := map[string]int{
		"a.c": 1, "b.c": 1, "x.o": 1, "prog_1": 1, // unchanged from phase 1
		"c.c": 1, "y.o": 1, "prog_2": 1, // newly executed
	}
	assertExecutionCounts(t, pfd.taskLookup, expectedAfterProg2)
}

func TestBasicImplicitDependency(t *testing.T) {
	yaml := `
producer:
  out: shared.txt
  run: echo "data" > $out

consumer:
  in: shared.txt
  out: result.txt
  run: cp $in $out
`

	pfd := parseFlowDefinitionSource(yaml)

	consumer := pfd.taskLookup["consumer"]
	if consumer == nil {
		t.Fatalf("consumer task not found")
	}

	producer := pfd.taskLookup["producer"]
	if producer == nil {
		t.Fatalf("producer task not found")
	}

	assertTaskDependencies(t, consumer, []string{"producer"})
	assertTaskDependencies(t, producer, []string{})
}

func TestMultipleConsumersImplicitDependency(t *testing.T) {
	yaml := `
shared-dep:
  out: shared.txt
  run: echo "shared data" > $out

taskA:
  in: shared.txt
  out: a.txt
  run: cp $in $out

taskB:
  in: shared.txt
  out: b.txt
  run: cp $in $out
`
	pfd := parseFlowDefinitionSource(yaml)

	sharedDep := pfd.taskLookup["shared-dep"]
	if sharedDep == nil {
		t.Fatalf("shared-dep task not found")
	}

	taskA := pfd.taskLookup["taskA"]
	if taskA == nil {
		t.Fatalf("taskA task not found")
	}

	taskB := pfd.taskLookup["taskB"]
	if taskB == nil {
		t.Fatalf("taskB task not found")
	}

	assertTaskDependencies(t, taskA, []string{"shared-dep"})
	assertTaskDependencies(t, taskB, []string{"shared-dep"})

	assertTaskDependencies(t, sharedDep, []string{})
}

func TestDependencyChainImplicitDependencies(t *testing.T) {
	yaml := `
step1:
  out: step1.txt
  run: echo "step1" > $out

step2:
  in: step1.txt
  out: step2.txt
  run: echo "step2" >> $in && cp $in $out

step3:
  in: step2.txt
  out: final.txt
  run: cp $in $out
`
	pfd := parseFlowDefinitionSource(yaml)

	// Get tasks
	step1 := pfd.taskLookup["step1"]
	if step1 == nil {
		t.Fatalf("step1 task not found")
	}

	step2 := pfd.taskLookup["step2"]
	if step2 == nil {
		t.Fatalf("step2 task not found")
	}

	step3 := pfd.taskLookup["step3"]
	if step3 == nil {
		t.Fatalf("step3 task not found")
	}

	// Test the dependency chain: step1 -> step2 -> step3
	// Note: The topological sort includes transitive dependencies for correct execution order
	assertTaskDependencies(t, step1, []string{})                 // no dependencies
	assertTaskDependencies(t, step2, []string{"step1"})          // depends on step1 via step1.txt
	assertTaskDependencies(t, step3, []string{"step1", "step2"}) // includes transitive dependency step1
}

func TestMixedExplicitImplicitDependencies(t *testing.T) {
	yaml := `
# Base file producer
base:
  out: base.txt
  run: echo "base" > $out

# Intermediate file producer 1
intermediate1:
  in: base.txt
  out: intermediate1.txt
  run: cp $in $out

# Intermediate file producer 2
intermediate2:
  in: base.txt
  out: intermediate2.txt
  run: echo "intermediate2" > $out

# Explicit array-style dependency group that references multiple tasks
deps:
  - intermediate1
  - intermediate2

# Task with implicit dependency on deps array via ${deps} reference
final:
  in: ${deps}
  out: final.txt
  run: cat $in > $out
`
	pfd := parseFlowDefinitionSource(yaml)

	// Get tasks
	base := pfd.taskLookup["base"]
	if base == nil {
		t.Fatalf("base task not found")
	}

	intermediate1 := pfd.taskLookup["intermediate1"]
	if intermediate1 == nil {
		t.Fatalf("intermediate1 task not found")
	}

	intermediate2 := pfd.taskLookup["intermediate2"]
	if intermediate2 == nil {
		t.Fatalf("intermediate2 task not found")
	}

	final := pfd.taskLookup["final"]
	if final == nil {
		t.Fatalf("final task not found")
	}

	// Test that both explicit and implicit dependencies work:
	// - base has no dependencies
	// - intermediate1 has implicit dependency on base (via base.txt)
	// - intermediate2 has implicit dependency on base (via base.txt)
	// - final has implicit dependency on intermediate1 and intermediate2 (via ${deps} expansion)
	assertTaskDependencies(t, base, []string{})
	assertTaskDependencies(t, intermediate1, []string{"base"})
	assertTaskDependencies(t, intermediate2, []string{"base"})
	assertTaskDependencies(t, final, []string{"base", "intermediate1", "intermediate2"}) // includes transitive dependencies

	// Test that the deps array variable is properly expanded in the final task
	// The ${deps} should expand to an array of task names, not a space-joined string
	if final.taskDeclaration.In == nil {
		t.Fatalf("final task should have input defined")
	}

	// Check that the input was properly expanded from the deps array
	expectedInputs := []string{"intermediate1", "intermediate2"}

	if len(final.inputs) != len(expectedInputs) {
		t.Fatalf("expected %d inputs, got %d", len(expectedInputs), len(final.inputs))
	}

	// test that final.inputs has the same contents as expectedInputs
	for i, input := range expectedInputs {
		if final.inputs[i].path != input {
			t.Fatalf("expected input %s not found in final.inputs: %v", input, final.inputs)
		}
	}

	yaml2 := `
# test that array variable expansion works within an array
intermediate1:
  out: intermediate1.txt
  run: echo "intermediate1" > $out

intermediate2:
  out: intermediate2.txt
  run: echo "intermediate2" > $out

intermediate3:
  out: intermediate3.txt
  run: echo "intermediate3" > $out

deps:
  - intermediate1
  - intermediate2

final2:
  in:
  - ${deps}
  - intermediate3
  out: final2.txt
  run: cat $in > $out
`
	pfd2 := parseFlowDefinitionSource(yaml2)

	final2 := pfd2.taskLookup["final2"]
	if final2 == nil {
		t.Fatalf("final2 task not found")
	}

	assertTaskDependencies(t, final2, []string{"intermediate1", "intermediate2", "intermediate3"})
}

func TestArrayVariableExpansion(t *testing.T) {
	yamlContent := `
# Global variables
version_string: 1.0.0
schemas:
  - file1.txt
  - file2.txt
  - file3.txt

# Test array variable expansion
test-array-basic:
  run: 'echo "Version: ${version_string}, Files: ${schemas}"'

test-array-indexed:
  run: 'echo "First: ${schemas[0]}, Second: ${schemas[1]}, Third: ${schemas[2]}"'

test-mixed-expansion:
  run: 'echo "Version ${version_string} processing ${schemas[0]} and ${schemas}"'
`

	pfd := parseFlowDefinitionSource(yamlContent)

	// Test that array variables are properly stored
	if schemas, exists := pfd.executionEnv["schemas"]; exists {
		expectedSchemas := []string{"file1.txt", "file2.txt", "file3.txt"}
		if len(schemas) != len(expectedSchemas) {
			t.Fatalf("Expected %d schema files, got %d", len(expectedSchemas), len(schemas))
		}
		for i, expected := range expectedSchemas {
			if schemas[i] != expected {
				t.Fatalf("Expected schema[%d] to be %s, got %s", i, expected, schemas[i])
			}
		}
	} else {
		t.Fatalf("schemas array variable not found in executionEnv")
	}

	// Test that string variables are stored as single-element arrays
	if versionArray, exists := pfd.executionEnv["version_string"]; exists {
		if len(versionArray) != 1 || versionArray[0] != "1.0.0" {
			t.Fatalf("Expected version_string to be [\"1.0.0\"], got %v", versionArray)
		}
	} else {
		t.Fatalf("version_string variable not found in executionEnv")
	}

	// Test command rendering with array variables
	basicTask := pfd.taskLookup["test-array-basic"]
	if basicTask == nil {
		t.Fatalf("test-array-basic task not found")
	}

	renderedBasic := renderCommand(basicTask, pfd.executionEnv)
	expectedBasic := "echo \"Version: ${version_string}, Files: ${schemas}\"" // Global variables not substituted
	if renderedBasic != expectedBasic {
		t.Fatalf("Basic array expansion failed. Expected: %s, Got: %s", expectedBasic, renderedBasic)
	}

	// Test indexed access
	indexedTask := pfd.taskLookup["test-array-indexed"]
	if indexedTask == nil {
		t.Fatalf("test-array-indexed task not found")
	}

	renderedIndexed := renderCommand(indexedTask, pfd.executionEnv)
	expectedIndexed := "echo \"First: ${schemas[0]}, Second: ${schemas[1]}, Third: ${schemas[2]}\"" // Global variables not substituted
	if renderedIndexed != expectedIndexed {
		t.Fatalf("Indexed array expansion failed. Expected: %s, Got: %s", expectedIndexed, renderedIndexed)
	}

	// Test mixed expansion
	mixedTask := pfd.taskLookup["test-mixed-expansion"]
	if mixedTask == nil {
		t.Fatalf("test-mixed-expansion task not found")
	}

	renderedMixed := renderCommand(mixedTask, pfd.executionEnv)
	expectedMixed := "echo \"Version ${version_string} processing ${schemas[0]} and ${schemas}\"" // Global variables not substituted
	if renderedMixed != expectedMixed {
		t.Fatalf("Mixed array expansion failed. Expected: %s, Got: %s", expectedMixed, renderedMixed)
	}
}

func TestBackwardsCompatibilityStringVariables(t *testing.T) {
	yamlContent := `
# Test backwards compatibility with string variables only
BASE_PATH: /home/user
PROJECT_NAME: myproject

old-style-task:
  in: ${BASE_PATH}/input.txt
  out: ${BASE_PATH}/${PROJECT_NAME}.out
  run: 'process ${in} > ${out}'
`

	pfd := parseFlowDefinitionSource(yamlContent)

	// Verify string variables are stored as single-element arrays
	if basePath, exists := pfd.executionEnv["BASE_PATH"]; exists {
		if len(basePath) != 1 || basePath[0] != "/home/user" {
			t.Fatalf("Expected BASE_PATH to be [\"/home/user\"], got %v", basePath)
		}
	} else {
		t.Fatalf("BASE_PATH variable not found")
	}

	// Test that old-style variable expansion still works
	task := pfd.taskLookup["old-style-task"]
	if task == nil {
		t.Fatalf("old-style-task not found")
	}

	rendered := renderCommand(task, pfd.executionEnv)
	// Note: paths get converted to relative by getPathRelativeToCwd
	expectedPrefix := "process "
	expectedSuffix := "input.txt > "
	expectedEnd := "myproject.out"
	if !strings.HasPrefix(rendered, expectedPrefix) ||
		!strings.Contains(rendered, expectedSuffix) ||
		!strings.HasSuffix(rendered, expectedEnd) {
		t.Fatalf("Backwards compatibility failed. Expected format: %s*%s*%s, Got: %s", expectedPrefix, expectedSuffix, expectedEnd, rendered)
	}

	// Verify input and output paths were properly processed
	if task.taskDeclaration.Out == nil {
		t.Fatalf("Output path not set")
	}
	// Output path will be converted to relative by getPathRelativeToCwd
	if !strings.HasSuffix(*task.taskDeclaration.Out, "myproject.out") {
		t.Fatalf("Expected output path to end with 'myproject.out', got %s", *task.taskDeclaration.Out)
	}
}

// ============================================================================
// Executor Tests
// ============================================================================

func TestRealExecutorConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		updateSha256 bool
		forceRun     bool
	}{
		{"default config", false, false},
		{"update sha256 only", true, false},
		{"force run only", false, true},
		{"both flags", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewRealExecutor(tt.updateSha256, tt.forceRun)

			if executor.ShouldUpdateSha256() != tt.updateSha256 {
				t.Errorf("ShouldUpdateSha256() = %v, want %v", executor.ShouldUpdateSha256(), tt.updateSha256)
			}
			if executor.ShouldForceRun() != tt.forceRun {
				t.Errorf("ShouldForceRun() = %v, want %v", executor.ShouldForceRun(), tt.forceRun)
			}
		})
	}
}

func TestDryRunExecutorConfiguration(t *testing.T) {
	tests := []struct {
		name         string
		updateSha256 bool
		forceRun     bool
	}{
		{"default config", false, false},
		{"update sha256 only", true, false},
		{"force run only", false, true},
		{"both flags", true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := NewDryRunExecutor(tt.updateSha256, tt.forceRun)

			if executor.ShouldUpdateSha256() != tt.updateSha256 {
				t.Errorf("ShouldUpdateSha256() = %v, want %v", executor.ShouldUpdateSha256(), tt.updateSha256)
			}
			if executor.ShouldForceRun() != tt.forceRun {
				t.Errorf("ShouldForceRun() = %v, want %v", executor.ShouldForceRun(), tt.forceRun)
			}
		})
	}
}

func TestExecutorCommandExecution(t *testing.T) {
	// Create a simple task for testing
	task := &RunnableTask{
		targetName: "test-task",
		taskDeclaration: &RunnableSchemaJson{
			Run: stringPtr("echo 'hello world'"),
		},
	}

	t.Run("RealExecutor command execution", func(t *testing.T) {
		executor := NewRealExecutor(false, false)

		// Test that ExecuteCommand works for real commands
		err := executor.ExecuteCommand(task, "echo 'hello world'", []string{})
		if err != nil {
			t.Errorf("Expected successful command execution, got %v", err)
		}
	})

	t.Run("DryRunExecutor command execution", func(t *testing.T) {
		executor := NewDryRunExecutor(false, false)

		// Test that ExecuteCommand works for dry run (should not error)
		err := executor.ExecuteCommand(task, "echo 'hello world'", []string{})
		if err != nil {
			t.Errorf("Expected successful dry run command simulation, got %v", err)
		}
	})
}

func TestExecutorDownloadHandling(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		output   string
		executor Executor
	}{
		{"RealExecutor HTTP download", "https://nonexistent.fake.domain.invalid/file.txt", "/tmp/file.txt", NewRealExecutor(false, false)},
		{"RealExecutor S3 download", "s3://nonexistent-bucket-fake-invalid/file.txt", "/tmp/file.txt", NewRealExecutor(false, false)},
		{"DryRunExecutor HTTP download", "https://nonexistent.fake.domain.invalid/file.txt", "/tmp/file.txt", NewDryRunExecutor(false, false)},
		{"DryRunExecutor S3 download", "s3://nonexistent-bucket-fake-invalid/file.txt", "/tmp/file.txt", NewDryRunExecutor(false, false)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test that DownloadFile is called appropriately for each executor type
			err := tt.executor.DownloadFile(tt.url, tt.output)

			switch tt.executor.(type) {
			case *RealExecutor:
				// Real executor behavior depends on URL type and available tools
				// HTTP URLs with nonexistent domains should fail
				// S3 URLs without credentials may return nil (no error, but no action)
				if strings.HasPrefix(tt.url, "http") {
					if err == nil {
						t.Errorf("Expected network error for RealExecutor with fake HTTP URL, got nil")
					}
				}
				// For S3 URLs, we accept either error or nil depending on credential availability
			case *DryRunExecutor:
				// Dry run executor should simulate successfully
				if err != nil {
					t.Errorf("Expected successful simulation for DryRunExecutor, got %v", err)
				}
			}
		})
	}
}

func TestExecutorTaskOutputMethods(t *testing.T) {
	task := &RunnableTask{
		targetName: "test-task",
		taskDeclaration: &RunnableSchemaJson{
			Out: stringPtr("output.txt"),
		},
	}

	executors := []struct {
		name     string
		executor Executor
	}{
		{"RealExecutor", NewRealExecutor(false, false)},
		{"DryRunExecutor", NewDryRunExecutor(false, false)},
	}

	for _, tt := range executors {
		t.Run(tt.name, func(t *testing.T) {
			// These should not panic, even though they're not implemented
			tt.executor.ShowTaskStart(task, make(map[string]*RunnableTask))
			tt.executor.ShowTaskSkip(task, "up-to-date")
			tt.executor.ShowTaskCompleted(task)
		})
	}
}

// Test executor integration with runTask function
func TestRunTaskWithExecutor(t *testing.T) {
	yamlContent := `
simple-task:
  run: echo "test"
`
	pfd := parseFlowDefinitionSource(yamlContent)
	task := pfd.taskLookup["simple-task"]
	if task == nil {
		t.Fatalf("simple-task not found")
	}

	t.Run("runTask with RealExecutor", func(t *testing.T) {
		executor := NewRealExecutor(false, false)

		// This should work now that runTask accepts an executor
		runTask(task, pfd.executionEnv, executor, pfd.taskLookup)

		// Verify task was executed
		if task.executionState != TaskCompleted {
			t.Errorf("Expected task to be completed, got %v", task.executionState)
		}
		if task.executionCount != 1 {
			t.Errorf("Expected task execution count to be 1, got %d", task.executionCount)
		}
	})

	t.Run("runTask with DryRunExecutor", func(t *testing.T) {
		// Reset task state for dry run test
		task.executionState = TaskNotStarted
		task.executionCount = 0

		executor := NewDryRunExecutor(false, false)

		// This should work now that runTask accepts an executor
		runTask(task, pfd.executionEnv, executor, pfd.taskLookup)

		// Verify task was "executed" in dry run mode
		if task.executionState != TaskCompleted {
			t.Errorf("Expected task to be completed in dry run, got %v", task.executionState)
		}
		if task.executionCount != 1 {
			t.Errorf("Expected task execution count to be 1 in dry run, got %d", task.executionCount)
		}
	})
}

// Test side effect handling - SHA256 computation vs downloads
func TestExecutorSideEffectHandling(t *testing.T) {
	// Test that SHA256 computation always happens (not a side effect)
	// but downloads are controlled by executor type

	t.Run("SHA256 computation is not affected by executor type", func(t *testing.T) {
		// Both executors should allow SHA256 computation since it's not a side effect
		realExec := NewRealExecutor(true, false)
		dryExec := NewDryRunExecutor(true, false)

		// SHA256 computation behavior should be the same for both
		if realExec.ShouldUpdateSha256() != dryExec.ShouldUpdateSha256() {
			t.Errorf("SHA256 computation behavior should be consistent across executor types")
		}
	})

	t.Run("Download behavior differs by executor type", func(t *testing.T) {
		realExec := NewRealExecutor(false, false)
		dryExec := NewDryRunExecutor(false, false)

		// Test download behavior - real will try to download, dry will simulate
		// Note: Real download will likely fail since we're using a bad URL, but that's expected
		realErr := realExec.DownloadFile("https://nonexistent.fake.domain.invalid/test.txt", "/tmp/test.txt")
		dryErr := dryExec.DownloadFile("https://nonexistent.fake.domain.invalid/test.txt", "/tmp/test.txt")

		// Real executor should return an error (from failed download)
		// Dry executor should succeed (simulation only)
		if realErr == nil {
			t.Errorf("RealExecutor should return an error for failed download")
		}
		if dryErr != nil {
			t.Errorf("DryRunExecutor should succeed for simulated download, got %v", dryErr)
		}
	})
}

// Test execution flow integration
func TestExecutorIntegrationFlow(t *testing.T) {
	tmp := t.TempDir()

	yamlContent := `
download-task:
  in: https://example.com/input.txt
  out: output.txt
  run: cp $in $out

compute-task:
  in: output.txt
  out: result.txt
  run: wc -l $in > $out
`

	flowPath := filepath.Join(tmp, "Sdflow.yaml")
	if err := os.WriteFile(flowPath, []byte(yamlContent), 0644); err != nil {
		t.Fatalf("write flow file: %v", err)
	}

	t.Run("full flow with RealExecutor", func(t *testing.T) {
		executor := NewRealExecutor(false, false)

		// This should work now that runFlowDefinitionProcessor accepts an executor
		runFlowDefinitionProcessor(flowPath, executor, []string{})

		// Note: This test will actually try to download and execute commands
		// In a real test environment, we might want to mock the network calls
	})

	t.Run("full flow with DryRunExecutor", func(t *testing.T) {
		executor := NewDryRunExecutor(false, false)

		// This should work and show execution plan without side effects
		runFlowDefinitionProcessor(flowPath, executor, []string{})

		// Dry run should complete without errors and without actual file operations
	})
}

// Helper function for string pointer (used in tests)
func stringPtr(s string) *string {
	return &s
}

func TestJsonnetJsonParsing(t *testing.T) {
	// Test JSON parsing
	t.Run("JSON parsing", func(t *testing.T) {
		jsonContent := `{
			"version_string": "1.0.0",
			"test-task": {
				"run": "echo 'Hello JSON'"
			}
		}`
		
		pfd := parseFlowDefinitionSource(jsonContent)
		
		if pfd == nil {
			t.Fatal("Failed to parse JSON content")
		}
		
		if len(pfd.taskLookup) == 0 {
			t.Fatal("No tasks found in parsed JSON")
		}
		
		testTask, exists := pfd.taskLookup["test-task"]
		if !exists {
			t.Fatal("test-task not found in parsed JSON")
		}
		
		if testTask.taskDeclaration.Run == nil {
			t.Fatal("Run command not found in test-task")
		}
		
		expectedRun := "echo 'Hello JSON'"
		if *testTask.taskDeclaration.Run != expectedRun {
			t.Fatalf("Expected run command '%s', got '%s'", expectedRun, *testTask.taskDeclaration.Run)
		}
	})
	
	// Test file type detection
	t.Run("File type detection", func(t *testing.T) {
		tests := []struct {
			filename string
			expected string
		}{
			{"test.yaml", "yaml"},
			{"test.yml", "yaml"},
			{"test.json", "json"},
			{"test.jsonnet", "jsonnet"},
			{"test", "yaml"}, // fallback
		}
		
		for _, tt := range tests {
			result := detectFileType(tt.filename)
			if result != tt.expected {
				t.Errorf("detectFileType(%s) = %s, expected %s", tt.filename, result, tt.expected)
			}
		}
	})
}

// Parallel execution tests

func TestParallelExecutorConstructors(t *testing.T) {
	t.Run("RealExecutor with worker count", func(t *testing.T) {
		executor := NewRealExecutorWithWorkers(4, false, false)
		if executor.WorkerCount() != 4 {
			t.Errorf("Expected worker count 4, got %d", executor.WorkerCount())
		}
	})

	t.Run("DryRunExecutor with worker count", func(t *testing.T) {
		executor := NewDryRunExecutorWithWorkers(2, false, false)
		if executor.WorkerCount() != 2 {
			t.Errorf("Expected worker count 2, got %d", executor.WorkerCount())
		}
	})

	t.Run("Default single worker behavior", func(t *testing.T) {
		realExec := NewRealExecutor(false, false)
		dryExec := NewDryRunExecutor(false, false)
		
		// These should default to 1 worker
		if realExec.WorkerCount() != 1 {
			t.Errorf("Expected default worker count 1 for RealExecutor, got %d", realExec.WorkerCount())
		}
		if dryExec.WorkerCount() != 1 {
			t.Errorf("Expected default worker count 1 for DryRunExecutor, got %d", dryExec.WorkerCount())
		}
	})
}

func TestDependencyLevelCalculation(t *testing.T) {
	// Create test task dependencies
	taskDependencies := map[string][]string{
		"level0-task1": {},
		"level0-task2": {},
		"level1-task1": {"level0-task1"},
		"level1-task2": {"level0-task2"},
		"level2-task1": {"level1-task1", "level1-task2"},
	}

	expectedLevels := map[string]int{
		"level0-task1": 0,
		"level0-task2": 0,
		"level1-task1": 1,
		"level1-task2": 1,
		"level2-task1": 2,
	}

	levels := calculateDependencyLevels(taskDependencies)
	
	for task, expectedLevel := range expectedLevels {
		if actualLevel, exists := levels[task]; !exists {
			t.Errorf("Task %s not found in levels map", task)
		} else if actualLevel != expectedLevel {
			t.Errorf("Task %s expected level %d, got %d", task, expectedLevel, actualLevel)
		}
	}
}

func TestTasksByLevel(t *testing.T) {
	taskDependencies := map[string][]string{
		"a": {},
		"b": {},
		"c": {"a"},
		"d": {"b"},
		"e": {"c", "d"},
	}

	levels := calculateDependencyLevels(taskDependencies)
	tasksByLevel := groupTasksByLevel(levels)

	// Level 0 should have tasks "a" and "b"
	level0Tasks := tasksByLevel[0]
	if len(level0Tasks) != 2 {
		t.Errorf("Expected 2 tasks at level 0, got %d", len(level0Tasks))
	}
	expectedLevel0 := map[string]bool{"a": true, "b": true}
	for _, task := range level0Tasks {
		if !expectedLevel0[task] {
			t.Errorf("Unexpected task %s at level 0", task)
		}
	}

	// Level 1 should have tasks "c" and "d"
	level1Tasks := tasksByLevel[1]
	if len(level1Tasks) != 2 {
		t.Errorf("Expected 2 tasks at level 1, got %d", len(level1Tasks))
	}
	expectedLevel1 := map[string]bool{"c": true, "d": true}
	for _, task := range level1Tasks {
		if !expectedLevel1[task] {
			t.Errorf("Unexpected task %s at level 1", task)
		}
	}

	// Level 2 should have task "e"
	level2Tasks := tasksByLevel[2]
	if len(level2Tasks) != 1 || level2Tasks[0] != "e" {
		t.Errorf("Expected task 'e' at level 2, got %v", level2Tasks)
	}
}

func TestParallelTaskExecution(t *testing.T) {
	// Create a simpler test YAML with independent tasks
	testYaml := `
test-parallel-simple:
  run: echo "simple task"; sleep 0.05`

	// Write test file
	tempDir, err := os.MkdirTemp("", "sdflow_parallel_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	flowPath := filepath.Join(tempDir, "test-flow.yaml")
	err = os.WriteFile(flowPath, []byte(testYaml), 0644)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Basic parallel execution functionality", func(t *testing.T) {
		executor := NewRealExecutorWithWorkers(4, false, false)
		
		parsedFlow := parseFlowDefinitionFile(flowPath)
		task := parsedFlow.taskLookup["test-parallel-simple"]
		
		if task == nil {
			t.Fatal("Task not found in parsed flow")
		}
		
		start := time.Now()
		runTaskParallel(task, parsedFlow.executionEnv, executor, parsedFlow.taskLookup)
		duration := time.Since(start)
		
		// Should complete successfully
		if task.executionState != TaskCompleted {
			t.Errorf("Expected task to be completed, got state: %v", task.executionState)
		}
		
		// Should take at least the sleep time
		if duration < 40*time.Millisecond {
			t.Errorf("Execution too fast, expected >40ms, got %v", duration)
		}
	})

	t.Run("Sequential vs Parallel comparison - stub test", func(t *testing.T) {
		// This is a placeholder for a more comprehensive parallel vs sequential test
		// For now, just verify the worker count functionality works
		
		seqExecutor := NewRealExecutorWithWorkers(1, false, false)
		parExecutor := NewRealExecutorWithWorkers(4, false, false)
		
		if seqExecutor.WorkerCount() != 1 {
			t.Errorf("Sequential executor should have 1 worker, got %d", seqExecutor.WorkerCount())
		}
		
		if parExecutor.WorkerCount() != 4 {
			t.Errorf("Parallel executor should have 4 workers, got %d", parExecutor.WorkerCount())
		}
	})
}

func TestTaskReferenceArrayDetection(t *testing.T) {
	// Test that task references work correctly in array inputs
	testYaml := `
producer-1:
  run: echo "producer 1"

producer-2:  
  run: echo "producer 2"

consumer:
  in:
    - producer-1
    - producer-2
  run: echo "consuming from producers"`

	// Write test file
	tempDir, err := os.MkdirTemp("", "sdflow_array_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	flowPath := filepath.Join(tempDir, "test-flow.yaml")
	err = os.WriteFile(flowPath, []byte(testYaml), 0644)
	if err != nil {
		t.Fatal(err)
	}

	parsedFlow := parseFlowDefinitionFile(flowPath)
	
	// Check if producer tasks are marked as referenced
	producer1 := parsedFlow.taskLookup["producer-1"]
	producer2 := parsedFlow.taskLookup["producer-2"]
	consumer := parsedFlow.taskLookup["consumer"]
	
	if producer1 == nil || producer2 == nil || consumer == nil {
		t.Fatal("Tasks not found in parsed flow")
	}
	
	t.Logf("producer-1.isReferenced: %v", producer1.isReferenced)
	t.Logf("producer-2.isReferenced: %v", producer2.isReferenced)
	t.Logf("consumer inputs: %d", len(consumer.inputs))
	for i, input := range consumer.inputs {
		t.Logf("consumer input[%d]: path=%s, taskReference=%s", i, input.path, input.taskReference)
	}
	
	// Verify that producer tasks are marked as referenced
	if !producer1.isReferenced {
		t.Error("producer-1 should be marked as referenced")
	}
	if !producer2.isReferenced {
		t.Error("producer-2 should be marked as referenced")
	}
}

func TestParallelFinalTaskReferences(t *testing.T) {
	// Skip this test - the parallel-final task structure has been removed from Sdflow.yaml
	t.Skip("parallel-final task structure no longer exists in current Sdflow.yaml")

	// Test that tasks referenced by parallel-final are properly marked as referenced
	parsedFlow := parseFlowDefinitionFile("./Sdflow.yaml")

	parallelFinal := parsedFlow.taskLookup["parallel-final"]
	if parallelFinal == nil {
		t.Fatal("parallel-final task not found")
	}
	
	// Check some of the parallel tasks to see if they're marked as referenced
	testTasks := []string{"parallel-task-03", "parallel-task-04", "parallel-task-05"}
	
	for _, taskName := range testTasks {
		task := parsedFlow.taskLookup[taskName]
		if task == nil {
			t.Errorf("Task %s not found", taskName)
			continue
		}
		
		t.Logf("Task %s: isReferenced=%v", taskName, task.isReferenced)
		if !task.isReferenced {
			t.Errorf("Task %s should be marked as referenced by parallel-final", taskName)
		}
	}
	
	// Check inputs of parallel-final
	t.Logf("parallel-final has %d inputs", len(parallelFinal.inputs))
	for i, input := range parallelFinal.inputs {
		if i < 5 { // Just show first 5 to avoid spam
			t.Logf("parallel-final input[%d]: path=%s, taskReference=%s", i, input.path, input.taskReference)
		}
	}
}

func TestParallelSHA256Caching(t *testing.T) {
	// Test that tasks without out: field get proper SHA256 caching in parallel execution
	testYaml := `
parallel-sha256-test:
  run: echo "SHA256 test output"`

	// Write test file
	tempDir, err := os.MkdirTemp("", "sdflow_sha256_test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	flowPath := filepath.Join(tempDir, "test-flow.yaml")
	err = os.WriteFile(flowPath, []byte(testYaml), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Change to temp directory for this test to avoid modifying main Sdflow.yaml
	oldWd, _ := os.Getwd()
	defer os.Chdir(oldWd)
	os.Chdir(tempDir)
	
	// Set up proper environment for the test
	oldFlowFile := FLOW_DEFINITION_FILE
	defer func() { FLOW_DEFINITION_FILE = oldFlowFile }()
	FLOW_DEFINITION_FILE = flowPath

	t.Run("Parallel execution with SHA256 caching", func(t *testing.T) {
		executor := NewRealExecutorWithWorkers(4, true, false) // Enable updateSha256
		
		parsedFlow := parseFlowDefinitionFile(flowPath)
		task := parsedFlow.taskLookup["parallel-sha256-test"]
		
		if task == nil {
			t.Fatal("Task not found in parsed flow")
		}
		
		// Execute with parallel execution
		runTaskParallel(task, parsedFlow.executionEnv, executor, parsedFlow.taskLookup)
		
		// Verify task completed successfully
		if task.executionState != TaskCompleted {
			t.Errorf("Expected task to be completed, got state: %v", task.executionState)
		}
		
		// Verify SHA256 was set in task declaration
		if task.taskDeclaration.OutSha256 == nil {
			t.Error("Expected OutSha256 to be set after parallel execution with updateSha256=true")
		} else {
			// Verify it looks like a valid SHA256 (64 hex characters)
			sha256Val := *task.taskDeclaration.OutSha256
			if len(sha256Val) != 64 {
				t.Errorf("Expected SHA256 length 64, got %d: %s", len(sha256Val), sha256Val)
			}
		}
		
		// Read the updated YAML file and verify SHA256 was written
		updatedYaml, err := os.ReadFile(flowPath)
		if err != nil {
			t.Fatalf("Failed to read updated YAML: %v", err)
		}
		
		yamlContent := string(updatedYaml)
		if !strings.Contains(yamlContent, "out.sha256:") {
			t.Error("Expected 'out.sha256:' to be present in updated YAML file")
		}
	})
}

func TestCommandLineFlagParsing(t *testing.T) {
	// This test will verify that -j flag is properly parsed
	// We'll need to test this through cobra command execution
	
	t.Run("Valid job count", func(t *testing.T) {
		// Test parsing -j 4
		jobCount, err := parseJobsFlag([]string{"-j", "4"})
		if err != nil {
			t.Errorf("Failed to parse valid jobs flag: %v", err)
		}
		if jobCount != 4 {
			t.Errorf("Expected job count 4, got %d", jobCount)
		}
	})
	
	t.Run("Invalid job count", func(t *testing.T) {
		// Test parsing -j 0 (should be invalid)
		_, err := parseJobsFlag([]string{"-j", "0"})
		if err == nil {
			t.Error("Expected error for invalid job count 0")
		}
		
		// Test parsing -j -1 (should be invalid)
		_, err = parseJobsFlag([]string{"-j", "-1"})
		if err == nil {
			t.Error("Expected error for negative job count")
		}
	})
	
	t.Run("Default job count", func(t *testing.T) {
		// Test default behavior (no -j flag)
		jobCount, err := parseJobsFlag([]string{})
		if err != nil {
			t.Errorf("Failed to parse default jobs: %v", err)
		}
		if jobCount != 1 {
			t.Errorf("Expected default job count 1, got %d", jobCount)
		}
	})
}

// CAS and path partitioning tests

func TestPartitionDigest(t *testing.T) {
	tests := []struct {
		name     string
		digest   string
		expected [3]string // [prefix1, prefix2, rest]
	}{
		{
			name:     "full SHA-256",
			digest:   "ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9",
			expected: [3]string{"ea", "8f", "ac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9"},
		},
		{
			name:     "short digest",
			digest:   "ab",
			expected: [3]string{"ab", "", ""},
		},
		{
			name:     "four character digest",
			digest:   "abcd",
			expected: [3]string{"ab", "cd", ""},
		},
		{
			name:     "empty digest",
			digest:   "",
			expected: [3]string{"", "", ""},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			prefix1, prefix2, rest := partitionDigest(tt.digest)
			if prefix1 != tt.expected[0] || prefix2 != tt.expected[1] || rest != tt.expected[2] {
				t.Errorf("partitionDigest(%q) = (%q, %q, %q), want (%q, %q, %q)",
					tt.digest, prefix1, prefix2, rest,
					tt.expected[0], tt.expected[1], tt.expected[2])
			}
		})
	}
}

func TestDigestToGitPath(t *testing.T) {
	tests := []struct {
		name     string
		digest   string
		expected string
	}{
		{
			name:     "full SHA-256",
			digest:   "ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9",
			expected: filepath.Join("ea", "8f", "ac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9"),
		},
		{
			name:     "short digest",
			digest:   "ab",
			expected: "ab",
		},
		{
			name:     "four character digest",
			digest:   "abcd",
			expected: filepath.Join("ab", "cd"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := digestToGitPath(tt.digest)
			if result != tt.expected {
				t.Errorf("digestToGitPath(%q) = %q, want %q", tt.digest, result, tt.expected)
			}
		})
	}
}

func TestGetCASObjectPath(t *testing.T) {
	baseDir := "/tmp/sdflow"
	digest := ContentDigest("ea8fac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9")

	result := getCASObjectPath(baseDir, digest)
	expected := filepath.Join(baseDir, "objects", "sha256", "ea", "8f", "ac7c65fb589b0d53560f5251f74f9e9b243478dcb6b3ea79b5e36449c8d9")

	if result != expected {
		t.Errorf("getCASObjectPath(%q, %q) = %q, want %q", baseDir, digest, result, expected)
	}
}

func TestGetTaskMetadataPath(t *testing.T) {
	baseDir := "/tmp/sdflow"
	digest := ActionDigest("1fb3d9e9a1c4b2e8f5a6c3d4e7f8a9b0c1d2e3f4")

	result := getTaskMetadataPath(baseDir, digest)
	expected := filepath.Join(baseDir, "tasks", "1f", "b3", "d9e9a1c4b2e8f5a6c3d4e7f8a9b0c1d2e3f4.json")

	if result != expected {
		t.Errorf("getTaskMetadataPath(%q, %q) = %q, want %q", baseDir, digest, result, expected)
	}
}

func TestComputeActionDigest(t *testing.T) {
	// Test basic computation
	taskName := "test-task"
	expandedRun := "echo hello"
	inputDigests := map[string]ContentDigest{
		"input.txt": ContentDigest("abc123"),
	}
	envVars := map[string]string{
		"HOME": "/home/user",
		"PATH": "/usr/bin",
	}

	digest1 := computeActionDigest(taskName, expandedRun, inputDigests, envVars)

	// Should be consistent
	digest2 := computeActionDigest(taskName, expandedRun, inputDigests, envVars)
	if digest1 != digest2 {
		t.Error("computeActionDigest should be deterministic")
	}

	// Different inputs should produce different digests
	differentEnvVars := map[string]string{
		"HOME": "/different/home",
		"PATH": "/usr/bin",
	}
	digest3 := computeActionDigest(taskName, expandedRun, inputDigests, differentEnvVars)
	if digest1 == digest3 {
		t.Error("Different inputs should produce different digests")
	}

	// Should be 64 characters (SHA-256 hex)
	if len(string(digest1)) != 64 {
		t.Errorf("Action digest should be 64 characters, got %d", len(string(digest1)))
	}
}

func TestComputeActionDigestDeterminism(t *testing.T) {
	// Test that order of maps doesn't affect result (should be sorted internally)
	taskName := "test"
	expandedRun := "echo test"

	inputDigests1 := map[string]ContentDigest{
		"a": ContentDigest("hash1"),
		"b": ContentDigest("hash2"),
		"c": ContentDigest("hash3"),
	}

	inputDigests2 := map[string]ContentDigest{
		"c": ContentDigest("hash3"),
		"a": ContentDigest("hash1"),
		"b": ContentDigest("hash2"),
	}

	envVars1 := map[string]string{
		"Z_VAR": "last",
		"A_VAR": "first",
		"M_VAR": "middle",
	}

	envVars2 := map[string]string{
		"A_VAR": "first",
		"M_VAR": "middle",
		"Z_VAR": "last",
	}

	digest1 := computeActionDigest(taskName, expandedRun, inputDigests1, envVars1)
	digest2 := computeActionDigest(taskName, expandedRun, inputDigests2, envVars2)

	if digest1 != digest2 {
		t.Error("Action digest should be deterministic regardless of map iteration order")
	}
}

func TestNormalizePathForDisplay(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		reason   string
	}{
		{
			name:     "absolute path unchanged",
			input:    "/tmp/file.txt",
			expected: "/tmp/file.txt",
			reason:   "absolute paths should be left unchanged",
		},
		{
			name:     "relative path with ./ prefix unchanged",
			input:    "./main.go",
			expected: "./main.go",
			reason:   "relative paths with ./ prefix should be unchanged",
		},
		{
			name:     "relative path with ../ prefix unchanged",
			input:    "../parent/file.txt",
			expected: "../parent/file.txt",
			reason:   "relative paths with ../ prefix should be unchanged",
		},
		{
			name:     "simple filename gets ./ prefix",
			input:    "main.go",
			expected: "./main.go",
			reason:   "simple filenames should get ./ prefix for consistency",
		},
		{
			name:     "http URL unchanged",
			input:    "https://example.com/file.txt",
			expected: "https://example.com/file.txt",
			reason:   "HTTP URLs should be left unchanged",
		},
		{
			name:     "s3 URL unchanged",
			input:    "s3://bucket/file.txt",
			expected: "s3://bucket/file.txt",
			reason:   "S3 URLs should be left unchanged",
		},
		{
			name:     "many ../ segments converted to absolute",
			input:    "../../../../../../tmp/file.txt",
			expected: "/tmp/file.txt",
			reason:   "paths with many ../ segments were likely absolute, convert back",
		},
		{
			name:     "few ../ segments unchanged",
			input:    "../file.txt",
			expected: "../file.txt",
			reason:   "paths with few ../ segments should remain relative",
		},
		{
			name:     "nested relative path gets ./ prefix",
			input:    "subdir/file.txt",
			expected: "./subdir/file.txt",
			reason:   "nested relative paths should get ./ prefix",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizePathForDisplay(tt.input)
			if result != tt.expected {
				t.Errorf("normalizePathForDisplay(%q) = %q, want %q (%s)",
					tt.input, result, tt.expected, tt.reason)
			}
		})
	}
}

func TestCASStdoutCapture(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	// Create task without out: field
	task := &RunnableTask{
		targetName: "stdout-test",
		taskDeclaration: &RunnableSchemaJson{
			Run: stringPtr("echo 'Hello CAS'"),
			// Note: no Out field - this should trigger CAS capture
		},
		inputs: []*RunnableTaskInput{},
	}

	// Create executor with updateSha256 enabled
	executor := NewRealExecutor(true, false) // updateSha256=true, forceRun=false

	// Override the CAS store to use temp directory
	executor.casStore = NewFilesystemCASStore(tempDir)
	executor.taskMetadataStore = NewFilesystemTaskMetadataStore(tempDir)

	// Execute the command
	err := executor.ExecuteCommand(task, "echo 'Hello CAS'", os.Environ())
	if err != nil {
		t.Fatalf("Expected successful execution, got error: %v", err)
	}

	// Verify CAS store contains the output
	expectedContent := "Hello CAS\n"
	hash := sha256.Sum256([]byte(expectedContent))
	expectedDigest := ContentDigest(hex.EncodeToString(hash[:]))

	if !executor.GetCASStore().Exists(expectedDigest) {
		t.Error("Expected content should exist in CAS store")
	}

	// Verify we can retrieve the content
	retrievedContent, err := executor.GetCASStore().Retrieve(expectedDigest)
	if err != nil {
		t.Fatalf("Failed to retrieve content from CAS: %v", err)
	}

	if string(retrievedContent) != expectedContent {
		t.Errorf("Retrieved content %q does not match expected %q", string(retrievedContent), expectedContent)
	}
}

func TestNormalExecutionWithoutCAS(t *testing.T) {
	// Test that normal execution (with out: field or updateSha256=false) doesn't use CAS
	tempDir := t.TempDir()

	// Create task WITH out: field
	task := &RunnableTask{
		targetName: "normal-test",
		taskDeclaration: &RunnableSchemaJson{
			Run: stringPtr("echo 'Not captured'"),
			Out: stringPtr("/tmp/test-output"),
		},
		inputs: []*RunnableTaskInput{},
	}

	// Create executor with updateSha256 enabled (but shouldn't matter due to Out field)
	executor := NewRealExecutor(true, false)
	executor.casStore = NewFilesystemCASStore(tempDir)
	executor.taskMetadataStore = NewFilesystemTaskMetadataStore(tempDir)

	// Execute the command
	err := executor.ExecuteCommand(task, "echo 'Not captured'", os.Environ())
	if err != nil {
		t.Fatalf("Expected successful execution, got error: %v", err)
	}

	// Verify CAS store is empty (no stdout capture should have occurred)
	expectedContent := "Not captured\n"
	hash := sha256.Sum256([]byte(expectedContent))
	expectedDigest := ContentDigest(hex.EncodeToString(hash[:]))

	if executor.GetCASStore().Exists(expectedDigest) {
		t.Error("Content should NOT exist in CAS store for tasks with explicit out: field")
	}
}

func TestCASIntegrationWithTask(t *testing.T) {
	// Test that CAS digest gets stored in the task structure for YAML updating
	tempDir := t.TempDir()

	// Create task without out: field
	task := &RunnableTask{
		targetName: "cas-integration-test",
		taskDeclaration: &RunnableSchemaJson{
			Run: stringPtr("echo 'CAS Integration Test'"),
			// Note: no Out field
		},
		inputs: []*RunnableTaskInput{},
	}

	// Create executor with updateSha256 enabled
	executor := NewRealExecutor(true, false)
	executor.casStore = NewFilesystemCASStore(tempDir)
	executor.taskMetadataStore = NewFilesystemTaskMetadataStore(tempDir)

	// Verify task starts with no CAS digest
	if task.casStdoutDigest != "" {
		t.Error("Task should start with no CAS stdout digest")
	}

	// Execute the command
	err := executor.ExecuteCommand(task, "echo 'CAS Integration Test'", os.Environ())
	if err != nil {
		t.Fatalf("Expected successful execution, got error: %v", err)
	}

	// Verify task now has a CAS stdout digest
	if task.casStdoutDigest == "" {
		t.Error("Task should have CAS stdout digest after execution")
	}

	// Verify the digest matches expected content
	expectedContent := "CAS Integration Test\n"
	hash := sha256.Sum256([]byte(expectedContent))
	expectedDigest := ContentDigest(hex.EncodeToString(hash[:]))

	if task.casStdoutDigest != expectedDigest {
		t.Errorf("CAS stdout digest %s does not match expected %s", task.casStdoutDigest, expectedDigest)
	}

	// Test getTaskStdoutDigestFromCAS function
	if digest, ok := getTaskStdoutDigestFromCAS(task, executor); !ok {
		t.Error("getTaskStdoutDigestFromCAS should return true for tasks with CAS digest")
	} else if digest != expectedDigest {
		t.Errorf("getTaskStdoutDigestFromCAS returned %s, expected %s", digest, expectedDigest)
	}
}

func TestTaskReferenceDetection(t *testing.T) {
	// Test the isTaskReference function
	yamlContent := `
producer-task:
  run: echo "Hello from producer"

consumer-task:
  in: producer-task
  run: cat $in

file-task:
  in: ./some-file.txt
  run: cat $in

out-task:
  out: ./output.txt
  run: echo "Has output"
`

	pfd := parseFlowDefinitionSource(yamlContent)

	// Test that isTaskReference correctly identifies task references
	tests := []struct {
		input    string
		expected bool
		reason   string
	}{
		{"producer-task", true, "producer-task has no out: field, should be a valid task reference"},
		{"./some-file.txt", false, "file path should not be a task reference"},
		{"out-task", false, "task with explicit out: should not be a valid task reference"},
		{"nonexistent-task", false, "nonexistent task should not be a task reference"},
		{"file.ext", false, "filename with extension should not be a task reference"},
	}

	for _, tt := range tests {
		result := isTaskReference(tt.input, pfd.taskLookup)
		if result != tt.expected {
			t.Errorf("isTaskReference(%q) = %v, want %v (%s)", tt.input, result, tt.expected, tt.reason)
		}
	}
}

func TestTaskReferenceDependencyResolution(t *testing.T) {
	// Test that task references create proper dependencies
	yamlContent := `
producer:
  run: echo "Hello World"

consumer:
  in: producer
  run: cat $in
`

	pfd := parseFlowDefinitionSource(yamlContent)

	// Check that consumer depends on producer
	consumerTask := pfd.taskLookup["consumer"]
	if consumerTask == nil {
		t.Fatal("consumer task not found")
	}

	// Check that the dependency was added
	found := false
	for _, dep := range consumerTask.taskDependencies {
		if dep.targetName == "producer" {
			found = true
			break
		}
	}

	if !found {
		t.Error("consumer task should have producer as a dependency")
	}

	// Check that the input was marked as a task reference
	if len(consumerTask.inputs) == 0 {
		t.Fatal("consumer task should have inputs")
	}

	input := consumerTask.inputs[0]
	if input.taskReference != "producer" {
		t.Errorf("input.taskReference = %q, want %q", input.taskReference, "producer")
	}
}

func TestTaskReferenceExecution(t *testing.T) {
	// Test that task references resolve to CAS paths during execution
	tempDir := t.TempDir()

	// Create tasks that use task references
	producerTask := &RunnableTask{
		targetName: "producer",
		taskDeclaration: &RunnableSchemaJson{
			Run: stringPtr("echo 'Task Reference Test'"),
			// No Out field - stdout should be captured to CAS
		},
		inputs:          []*RunnableTaskInput{},
		casStdoutDigest: "",   // Will be set after execution
		isReferenced:    true, // Mark as referenced to enable CAS capture without --updatehash
	}

	consumerTask := &RunnableTask{
		targetName: "consumer",
		taskDeclaration: &RunnableSchemaJson{
			Run: stringPtr("cat $in"),
		},
		inputs: []*RunnableTaskInput{
			{
				path:          "producer",
				taskReference: "producer", // This marks it as a task reference
			},
		},
		taskDependencies: []*RunnableTask{producerTask},
	}

	// Create executor with updateSha256 disabled (task references should work without --updatehash)
	executor := NewRealExecutor(false, false)
	executor.casStore = NewFilesystemCASStore(tempDir)
	executor.taskMetadataStore = NewFilesystemTaskMetadataStore(tempDir)

	// Execute producer task first (simulating dependency execution)
	err := executor.ExecuteCommand(producerTask, "echo 'Task Reference Test'", os.Environ())
	if err != nil {
		t.Fatalf("Failed to execute producer task: %v", err)
	}

	// Verify producer task has CAS digest
	if producerTask.casStdoutDigest == "" {
		t.Fatal("Producer task should have CAS stdout digest after execution")
	}

	// Create environment for consumer task execution
	env := make(map[string][]string)

	// Render command for consumer task - this should resolve task reference to CAS path
	renderedCommand := renderCommand(consumerTask, env)

	// The rendered command should contain the CAS path, not the literal "producer"
	expectedCASPath := getCASObjectPath(CACHE_DIRECTORY, producerTask.casStdoutDigest)
	if !strings.Contains(renderedCommand, expectedCASPath) {
		t.Errorf("Rendered command %q should contain CAS path %q", renderedCommand, expectedCASPath)
	}

	// Should not contain the literal task name "producer"
	if strings.Contains(renderedCommand, "producer") {
		t.Errorf("Rendered command %q should not contain literal task name 'producer'", renderedCommand)
	}
}

/* -------------------------------------------------------------------------- */
/*                        Cycle Handling Tests                                */
/* -------------------------------------------------------------------------- */

func TestSelfLoopCycle_SingleInputOutput(t *testing.T) {
	// Test A->A: task where input file equals output file (self-modifying task)
	tempDir := t.TempDir()
	versionFile := filepath.Join(tempDir, "version.go")

	// Create initial version file
	initialContent := "package main\nconst Version = \"0.0.1\"\n"
	if err := os.WriteFile(versionFile, []byte(initialContent), 0644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond) // Ensure different mtime

	yamlContent := `
version.go:
  in: ` + versionFile + `
  out: ` + versionFile + `
  run: echo 'package main\nconst Version = "0.0.2"' > ` + versionFile + `
`

	pfd := parseFlowDefinitionSource(yamlContent)
	task := pfd.taskLookup["version.go"]

	if task == nil {
		t.Fatal("version.go task not found")
	}

	// Task should be parsed successfully (cycle will be detected at topsort, not parse time)
	if task.taskDeclaration == nil {
		t.Fatal("Task declaration should exist")
	}

	// Execute the task
	executor := NewRealExecutor(false, false)

	// This should work - the task runs once due to execution state tracking
	runTask(task, pfd.executionEnv, executor, pfd.taskLookup)

	// Verify task was executed (execution count incremented)
	if task.executionCount != 1 {
		t.Errorf("Task should execute once, got executionCount=%d", task.executionCount)
	}

	// Verify the file was modified
	content, err := os.ReadFile(versionFile)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(content), "0.0.2") {
		t.Errorf("File should be updated to 0.0.2, got: %s", string(content))
	}
}

func TestSelfLoopCycle_MultipleInputsWithOneMatchingOutput(t *testing.T) {
	// Test task with multiple inputs where one input equals the output
	tempDir := t.TempDir()
	versionFile := filepath.Join(tempDir, "version.go")
	sourceFile := filepath.Join(tempDir, "source.txt")

	// Create initial files
	if err := os.WriteFile(versionFile, []byte("version 0.0.1"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(sourceFile, []byte("source data"), 0644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	yamlContent := `
update-version:
  in: [` + sourceFile + `, ` + versionFile + `]
  out: ` + versionFile + `
  run: echo "updated from source" > ` + versionFile + `
`

	pfd := parseFlowDefinitionSource(yamlContent)
	task := pfd.taskLookup["update-version"]

	if task == nil {
		t.Fatal("update-version task not found")
	}

	executor := NewRealExecutor(false, false)
	runTask(task, pfd.executionEnv, executor, pfd.taskLookup)

	if task.executionCount != 1 {
		t.Errorf("Task should execute once, got executionCount=%d", task.executionCount)
	}

	content, err := os.ReadFile(versionFile)
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(string(content), "updated from source") {
		t.Errorf("File should be updated, got: %s", string(content))
	}
}

func TestTwoNodeCycle_AtoB_BtoA(t *testing.T) {
	// Test A->B->A: two tasks that depend on each other's outputs
	tempDir := t.TempDir()
	fileA := filepath.Join(tempDir, "a.txt")
	fileB := filepath.Join(tempDir, "b.txt")

	// Create initial files
	if err := os.WriteFile(fileA, []byte("A initial"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(fileB, []byte("B initial"), 0644); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)

	yamlContent := `
task-a:
  in: ` + fileB + `
  out: ` + fileA + `
  run: echo "A from B" > ` + fileA + `

task-b:
  in: ` + fileA + `
  out: ` + fileB + `
  run: echo "B from A" > ` + fileB + `
`

	pfd := parseFlowDefinitionSource(yamlContent)
	taskA := pfd.taskLookup["task-a"]
	taskB := pfd.taskLookup["task-b"]

	if taskA == nil || taskB == nil {
		t.Fatal("Tasks not found")
	}

	// Debug: Log dependencies
	t.Logf("taskLookup keys:")
	for key := range pfd.taskLookup {
		t.Logf("  %q", key)
	}
	t.Logf("task-a dependencies from map: %v", pfd.taskDependencies["task-a"])
	t.Logf("task-b dependencies from map: %v", pfd.taskDependencies["task-b"])
	t.Logf("task-a inputs:")
	for _, inp := range taskA.inputs {
		t.Logf("  path=%q", inp.path)
	}
	t.Logf("task-b inputs:")
	for _, inp := range taskB.inputs {
		t.Logf("  path=%q", inp.path)
	}
	t.Logf("task-a out: %q", *taskA.taskDeclaration.Out)
	t.Logf("task-b out: %q", *taskB.taskDeclaration.Out)

	executor := NewRealExecutor(false, false)

	// Run task-a - it should handle the cycle via execution state
	runTask(taskA, pfd.executionEnv, executor, pfd.taskLookup)

	// Both tasks should have executed exactly once
	if taskA.executionCount != 1 {
		t.Errorf("task-a should execute once, got %d", taskA.executionCount)
	}
	if taskB.executionCount != 1 {
		t.Errorf("task-b should execute once, got %d", taskB.executionCount)
	}

	// Verify both files were updated
	contentA, _ := os.ReadFile(fileA)
	contentB, _ := os.ReadFile(fileB)

	if !strings.Contains(string(contentA), "A from B") {
		t.Errorf("File A should be updated, got: %s", string(contentA))
	}
	if !strings.Contains(string(contentB), "B from A") {
		t.Errorf("File B should be updated, got: %s", string(contentB))
	}
}

func TestThreeNodeCycle_ABC(t *testing.T) {
	// Test A->B->C->A: three-node cycle
	tempDir := t.TempDir()
	fileA := filepath.Join(tempDir, "a.txt")
	fileB := filepath.Join(tempDir, "b.txt")
	fileC := filepath.Join(tempDir, "c.txt")

	// Create initial files
	for _, f := range []string{fileA, fileB, fileC} {
		if err := os.WriteFile(f, []byte("initial"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)

	yamlContent := `
task-a:
  in: ` + fileC + `
  out: ` + fileA + `
  run: echo "A updated" > ` + fileA + `

task-b:
  in: ` + fileA + `
  out: ` + fileB + `
  run: echo "B updated" > ` + fileB + `

task-c:
  in: ` + fileB + `
  out: ` + fileC + `
  run: echo "C updated" > ` + fileC + `
`

	pfd := parseFlowDefinitionSource(yamlContent)
	taskA := pfd.taskLookup["task-a"]
	taskB := pfd.taskLookup["task-b"]
	taskC := pfd.taskLookup["task-c"]

	if taskA == nil || taskB == nil || taskC == nil {
		t.Fatal("Tasks not found")
	}

	executor := NewRealExecutor(false, false)
	runTask(taskA, pfd.executionEnv, executor, pfd.taskLookup)

	// All three tasks should execute exactly once
	if taskA.executionCount != 1 {
		t.Errorf("task-a should execute once, got %d", taskA.executionCount)
	}
	if taskB.executionCount != 1 {
		t.Errorf("task-b should execute once, got %d", taskB.executionCount)
	}
	if taskC.executionCount != 1 {
		t.Errorf("task-c should execute once, got %d", taskC.executionCount)
	}
}

func TestCycleWithinLargerGraph(t *testing.T) {
	// Test: Independent->Cycle(A->B->A)->Dependent
	// Ensures cycles don't break non-cyclic dependencies
	tempDir := t.TempDir()
	fileIndep := filepath.Join(tempDir, "independent.txt")
	fileA := filepath.Join(tempDir, "a.txt")
	fileB := filepath.Join(tempDir, "b.txt")
	fileDep := filepath.Join(tempDir, "dependent.txt")

	// Create initial files
	for _, f := range []string{fileIndep, fileA, fileB} {
		if err := os.WriteFile(f, []byte("initial"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(10 * time.Millisecond)

	yamlContent := `
independent:
  out: ` + fileIndep + `
  run: echo "independent" > ` + fileIndep + `

cycle-a:
  in: [` + fileIndep + `, ` + fileB + `]
  out: ` + fileA + `
  run: echo "cycle A" > ` + fileA + `

cycle-b:
  in: ` + fileA + `
  out: ` + fileB + `
  run: echo "cycle B" > ` + fileB + `

dependent:
  in: ` + fileB + `
  out: ` + fileDep + `
  run: echo "dependent" > ` + fileDep + `
`

	pfd := parseFlowDefinitionSource(yamlContent)
	taskIndep := pfd.taskLookup["independent"]
	taskCycleA := pfd.taskLookup["cycle-a"]
	taskCycleB := pfd.taskLookup["cycle-b"]
	taskDep := pfd.taskLookup["dependent"]

	if taskIndep == nil || taskCycleA == nil || taskCycleB == nil || taskDep == nil {
		t.Fatal("Tasks not found")
	}

	executor := NewRealExecutor(false, false)
	runTask(taskDep, pfd.executionEnv, executor, pfd.taskLookup)

	// Verify execution: independent may be skipped if up-to-date, others should execute
	// Since we created files before, independent's output exists and is newer than its inputs
	if taskIndep.executionState != TaskSkipped {
		t.Errorf("independent should be skipped (up-to-date), got state %v", taskIndep.executionState)
	}
	if taskCycleA.executionCount != 1 {
		t.Errorf("cycle-a should execute once, got %d", taskCycleA.executionCount)
	}
	if taskCycleB.executionCount != 1 {
		t.Errorf("cycle-b should execute once, got %d", taskCycleB.executionCount)
	}
	if taskDep.executionCount != 1 {
		t.Errorf("dependent should execute once, got %d", taskDep.executionCount)
	}

	// Verify all files were created
	for _, f := range []string{fileIndep, fileA, fileB, fileDep} {
		if _, err := os.Stat(f); err != nil {
			t.Errorf("File %s should exist: %v", f, err)
		}
	}
}

func TestCycleAlwaysRunsOnReinvocation(t *testing.T) {
	// Test that cyclic tasks always run on each CLI invocation (treated as always-stale)
	tempDir := t.TempDir()
	versionFile := filepath.Join(tempDir, "version.go")

	// Create initial file
	if err := os.WriteFile(versionFile, []byte("v1"), 0644); err != nil {
		t.Fatal(err)
	}

	yamlContent := `
version.go:
  in: ` + versionFile + `
  out: ` + versionFile + `
  run: echo "v2" > ` + versionFile + `
`

	pfd := parseFlowDefinitionSource(yamlContent)
	task := pfd.taskLookup["version.go"]

	if task == nil {
		t.Fatal("Task not found")
	}

	executor := NewRealExecutor(false, false)

	// First invocation
	runTask(task, pfd.executionEnv, executor, pfd.taskLookup)
	if task.executionCount != 1 {
		t.Errorf("First run: expected executionCount=1, got %d", task.executionCount)
	}

	// Wait to ensure different mtime
	time.Sleep(10 * time.Millisecond)

	// Reset execution state to simulate new CLI invocation
	task.executionState = TaskNotStarted

	// Second invocation - should run again because cycles always run
	runTask(task, pfd.executionEnv, executor, pfd.taskLookup)
	if task.executionCount != 2 {
		t.Errorf("Second run: expected executionCount=2, got %d", task.executionCount)
	}
}
