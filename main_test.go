package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		env         map[string]string
		wantCommand string
	}{
		{
			name: "single input file",
			runnable: map[string]interface{}{
				"run": "cp $in $out",
				"out": "dst.txt",
				"in":  "src.txt",
			},
			env:         map[string]string{},
			wantCommand: "cp src.txt dst.txt",
		},
		{
			name: "multiple input files",
			runnable: map[string]interface{}{
				"run": "cp ${in[0]} $out",
				"out": "dst.txt",
				"in":  stringSliceToInterface([]string{"src1.txt", "source two.text", "source-3.another.file"}),
			},
			env:         map[string]string{},
			wantCommand: "cp src1.txt dst.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			task := createTaskFromRunnableKeyVals(tt.runnable, tt.env)
			got := renderCommand(task)
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
	if task.taskDeclaration == nil || task.taskDeclaration.Out == nil ||
		*task.taskDeclaration.Out != "./implied-file.dat" {
		t.Fatalf("out path wrong: %+v", task.taskDeclaration)
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

	renderedCommand := renderCommand(task)
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

	renderedCommand := renderCommand(task)

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
