package main

import (
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"embed"

	"github.com/fatih/color"
	"github.com/santhosh-tekuri/jsonschema/v5"
	"gopkg.in/yaml.v3"
)

const FLOW_DEFINITION_FILE = "sdflow.yaml"

//go:embed schemas/sdflow.yaml.schema.json
var sdFlowSchema embed.FS

var data = `
a: Easy!
b:
  c: 2
  d: [3, 4]
`

// Note: struct fields must be public in order for unmarshal to
// correctly populate the data.
type T struct {
	A string
	B struct {
		RenamedC int   `yaml:"c"`
		D        []int `yaml:",flow"`
	}
}

func sample1() {
	t := T{}

	err := yaml.Unmarshal([]byte(data), &t)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- t:\n%v\n\n", t)

	d, err := yaml.Marshal(&t)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- t dump:\n%s\n\n", string(d))

	m := make(map[interface{}]interface{})

	err = yaml.Unmarshal([]byte(data), &m)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- m:\n%v\n\n", m)

	d, err = yaml.Marshal(&m)
	if err != nil {
		log.Fatalf("error: %v", err)
	}
	fmt.Printf("--- m dump:\n%s\n\n", string(d))
}

// Define structs to match the YAML structure
type CloudMakeConfig struct {
	Env      map[string]string  `yaml:"env"`
	Projects map[string]Project `yaml:",inline"`
}

type Project struct {
	Out  string `yaml:"out"`
	In   string `yaml:"in"`
	Pre  string `yaml:"pre"`
	Run  string `yaml:"run"`
	Post string `yaml:"post"`
	// Config ProjectConfig `yaml:"config"`
	Config interface{} `yaml:"config"`
}

// type ProjectConfig struct {
// 	S3     S3Config     `yaml:"s3"`
// 	Notify NotifyConfig `yaml:"notify"`
// }

// type S3Config struct {
// 	Retry int `yaml:"retry"`
// }

// type NotifyConfig struct {
// 	Failure FailureNotify `yaml:"failure"`
// }

// type FailureNotify struct {
// 	Slack SlackConfig `yaml:"slack"`
// }

// type SlackConfig struct {
// 	Message string `yaml:"message"`
// 	Channel string `yaml:"channel"`
// }

type RunnableTask struct {
	taskDeclaration  *RunnableSchemaJson
	taskDependencies []*RunnableTask
	targetName       string
}

func validateFlowDefinitionFile(flowDefinitionFile string) {
	var flowDefinitionObject map[string]interface{}
	flowDefinitionSource, err := os.ReadFile(FLOW_DEFINITION_FILE)
	bailOnError(err)
	if err := yaml.Unmarshal([]byte(flowDefinitionSource), &flowDefinitionObject); err != nil {
		log.Fatalf("FAILED TO READ YAML\nerror: %v", err)
	}
	validatorSchemaSource, err := fs.ReadFile(sdFlowSchema, "schemas/sdflow.yaml.schema.json")
	bailOnError(err)
	validator := jsonschema.MustCompileString("schemas/sdflow.yaml.schema.json", string(validatorSchemaSource))
	if err := validator.Validate(flowDefinitionObject); err != nil {
		log.Fatalf("SDFLOW FAILED TO VALIDATE\nerror: %v", err)
	}
}

func printVitalsForTask(task *RunnableTask) {
	if task.taskDeclaration == nil {
		return
	}

	fmt.Fprint(os.Stderr, color.CyanString("Task: %s --> ", task.targetName))

	if task.taskDeclaration.Out == nil {
		fmt.Fprint(
			os.Stderr,
			color.CyanString("%s\n", "<STDOUT>"),
		)
	} else {
		fmt.Fprint(
			os.Stderr,
			color.YellowString("%s\n", *task.taskDeclaration.Out),
		)
	}

}

func substituteWithContext(s string, context map[string]string) *string {
	mapper := func(varName string) string {
		return context[varName]
	}

	substituted := os.Expand(s, mapper)
	return &substituted
}

func renderCommand(runnable *RunnableSchemaJson) string {

	vars := map[string]string{}

	if runnable.In != nil {
		vars["in"] = *runnable.In
	}
	if runnable.Out != nil {
		vars["out"] = *runnable.Out
	}

	mapper := func(varName string) string {
		return vars[varName]
	}

	renderedCommand := os.Expand(runnable.Run, mapper)
	return renderedCommand
}

func runTask(task *RunnableTask, env map[string]string) {
	fmt.Printf("Running task: %+v (%d dependencies)\n", task.targetName, len(task.taskDependencies))

	for _, dep := range task.taskDependencies {
		fmt.Println("Running dependency:", dep.targetName)
		runTask(dep, env)
	}

	if task.taskDeclaration == nil {
		fmt.Println(color.RedString("No task declaration found!!!"))
		return
	}

	command := renderCommand(task.taskDeclaration)
	fmt.Fprint(
		os.Stderr,
		color.GreenString("Command: %s\n", command),
	)

	cmd := exec.Command("bash", "-c", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	var cmdEnv []string
	cmdEnv = append(cmdEnv, os.Environ()...)
	for key, value := range env {
		cmdEnv = append(cmdEnv, fmt.Sprintf("%s=%s", key, value))
	}
	cmd.Env = cmdEnv

	err := cmd.Run()
	if err != nil {
		fmt.Println("Error executing command:", err)
		return
	}
}

func getPathRelativeToCwd(path string) string {
	cwd, err := os.Getwd()
	bailOnError(err)
	absPath, err := filepath.Abs(path)
	bailOnError(err)
	relPath, err := filepath.Rel(cwd, absPath)
	bailOnError(err)
	return relPath
}

func sample4() {

	var flowDefinitionObject map[string]interface{}
	flowDefinitionSource, err := os.ReadFile(FLOW_DEFINITION_FILE)
	bailOnError(err)
	if err := yaml.Unmarshal([]byte(flowDefinitionSource), &flowDefinitionObject); err != nil {
		log.Fatalf("error: %v", err)
	}

	executionEnv := make(map[string]string)

	// first pass: compile the execution environment
	for targetIdentifier, value := range flowDefinitionObject {

		switch value.(type) {
		case string: // variable definitions
			executionEnv[targetIdentifier] = flowDefinitionObject[targetIdentifier].(string)
		}
	}

	taskLookup := make(map[string]*RunnableTask)
	taskDependencies := make(map[string][]string)

	// second pass: retrieve tasks and substitute using executionEnv
	for targetIdentifier, value := range flowDefinitionObject {

		if executionEnv[targetIdentifier] != "" {
			// skip variable definitions
			continue
		}
		substitutedTargetName := *substituteWithContext(targetIdentifier, executionEnv)

		// ensure the target is in the dependency tracker
		if _, ok := taskDependencies[substitutedTargetName]; !ok {
			taskDependencies[substitutedTargetName] = make([]string, 0)
		}

		switch ruleContent := value.(type) {

		case string: // variable definitions
			continue

		case []interface{}: // compile subtargets
			for _, subTarget := range ruleContent {
				taskDependencies[substitutedTargetName] = append(taskDependencies[substitutedTargetName],
					*substituteWithContext(subTarget.(string), executionEnv))
			}
			task := RunnableTask{
				targetName: substitutedTargetName,
			}
			taskLookup[substitutedTargetName] = &task

		default: // all other cases should be map
			runnableData := ruleContent.(map[string]interface{})

			task := RunnableTask{
				taskDeclaration: &RunnableSchemaJson{},
			}

			task.targetName = *substituteWithContext(substitutedTargetName, executionEnv)
			if isPath(task.targetName) {
				fileAbsPath := getPathRelativeToCwd(task.targetName)
				task.taskDeclaration.Out = &fileAbsPath
			} else {
				if outputPathValue, ok := runnableData["out"]; ok {
					fileAbsPath := getPathRelativeToCwd(
						*substituteWithContext(outputPathValue.(string), executionEnv))
					task.taskDeclaration.Out = &fileAbsPath
				}
			}

			if inValue, ok := runnableData["in"]; ok {
				inString := getPathRelativeToCwd(
					*substituteWithContext(inValue.(string), executionEnv))
				task.taskDeclaration.In = &inString
			} else {
				log.Fatalf("error: %v", "run is required")
			}

			if runnableValue, ok := runnableData["run"]; ok {
				runString := runnableValue.(string)
				task.taskDeclaration.Run = runString
			} else {
				log.Fatalf("error: %v", "run is required")
			}

			taskLookup[substitutedTargetName] = &task
		}
	}

	// populate the dependencies
	for targetIdentifier := range taskDependencies {
		task := taskLookup[targetIdentifier]
		topSortedDependencies := topSortDependencies(taskDependencies, targetIdentifier)
		for _, dep := range topSortedDependencies[:len(topSortedDependencies)-1] {
			depTask := taskLookup[dep]
			if depTask.taskDeclaration == nil {
				bailOnError(fmt.Errorf("subtask %s has no definition!?", dep))
			}
			task.taskDependencies = append(task.taskDependencies, depTask)
		}
	}

	for targetIdentifier := range taskDependencies {
		task := taskLookup[targetIdentifier]
		printVitalsForTask(task)
	}

	lastArg := os.Args[len(os.Args)-1]
	// see if lastarg is in our lookup
	if _, ok := taskLookup[lastArg]; !ok {
		fmt.Printf("Task %s not found\n", lastArg)
		return
	} else {
		task := taskLookup[lastArg]
		runTask(task, executionEnv)
	}
}

func main() {
	validateFlowDefinitionFile(FLOW_DEFINITION_FILE)

	// sample1()
	// sample3()
	sample4()

}

func isPath(s string) bool {
	return strings.HasPrefix(s, "./") || strings.HasPrefix(s, "/")
}
