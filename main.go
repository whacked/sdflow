package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"embed"

	"github.com/spf13/cobra"

	"github.com/fatih/color"
	"github.com/santhosh-tekuri/jsonschema/v5"
	yaml "gopkg.in/yaml.v3"
)

const FLOW_DEFINITION_FILE = "sdflow.yaml"
const CACHE_DIRECTORY = ".sdflow.cache"

//go:embed schemas/sdflow.yaml.schema.json
var sdFlowSchema embed.FS

type RunnableTask struct {
	taskDeclaration  *RunnableSchemaJson
	taskDependencies []*RunnableTask
	targetName       string
	inTime           int64
	outTime          int64
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

	var upToDateString string
	if task.outTime > task.inTime {
		upToDateString = color.GreenString("current")
	} else if task.taskDeclaration.Out == nil {
		upToDateString = color.MagentaString("always ")
	} else {
		upToDateString = color.RedString("stale  ")
	}

	fmt.Fprintf(os.Stderr,
		"%s [%s]\n  %s --> ",
		upToDateString,
		color.HiWhiteString("%s", task.targetName),
		color.YellowString("%s", *task.taskDeclaration.In),
	)

	if task.taskDeclaration.Out == nil {
		fmt.Fprint(
			os.Stderr,
			color.CyanString("%s", "<STDOUT>"),
		)
	} else {
		fmt.Fprintf(
			os.Stderr,
			"%s",
			color.BlueString("%s", *task.taskDeclaration.Out),
		)
	}

	fmt.Fprintf(os.Stderr, "\n")
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

func getTaskInputCachePath(task *RunnableTask) string {
	if task.taskDeclaration.InSha256 != nil {
		return filepath.Join(CACHE_DIRECTORY, *task.taskDeclaration.InSha256)
	}
	return ""
}

func isTaskInputInCache(task *RunnableTask) bool {
	if task.taskDeclaration.InSha256 != nil {
		cachePath := getTaskInputCachePath(task)
		if _, err := os.Stat(cachePath); err == nil {
			return true
		}
	}
	return false
}

func saveTaskInputToCache(task *RunnableTask) string {
	if _, err := os.Stat(CACHE_DIRECTORY); os.IsNotExist(err) {
		os.Mkdir(CACHE_DIRECTORY, 0755)
	}

	remoteBytes := getRemoteResourceBytes(*task.taskDeclaration.In)
	fmt.Println("Downloaded content length:", len(remoteBytes))
	if isBytesMatchingSha256(remoteBytes, *task.taskDeclaration.InSha256) {
		fmt.Println("SHA256 matches!")
	} else {
		fmt.Println("SHA256 mismatch!")
	}
	cachePath := getTaskInputCachePath(task)
	err := os.WriteFile(cachePath, remoteBytes, 0644)
	bailOnError(err)
	return cachePath
}

func isBytesMatchingSha256(bytes []byte, precomputedSha256 string) bool {
	bytesSha256 := sha256.Sum256(bytes)
	hexValue := hex.EncodeToString(bytesSha256[:])
	return hexValue == precomputedSha256
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

	if task.taskDeclaration.InSha256 != nil && task.taskDeclaration.In != nil {
		fmt.Println("Checking sha256 of input file")

		if isRemotePath(*task.taskDeclaration.In) {

			if isTaskInputInCache(task) {
				cachedInputPath := getTaskInputCachePath(task)
				fmt.Println("Using cached input", cachedInputPath)
				return
			} else {
				cachedInputPath := saveTaskInputToCache(task)
				fmt.Println("saved input to cache", cachedInputPath)
				return
			}
		}
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

	if task.taskDeclaration.OutSha256 != nil {
		outFileBytes, err := os.ReadFile(*task.taskDeclaration.Out)
		bailOnError(err)

		if isBytesMatchingSha256(outFileBytes, *task.taskDeclaration.OutSha256) {
			fmt.Println("OUT SHA256 matches!")
		} else {
			fmt.Println("OUT SHA256 mismatch!")
		}
	}
}

func getPathRelativeToCwd(path string) string {
	if !isPath(path) {
		return path
	}
	cwd, err := os.Getwd()
	bailOnError(err)
	absPath, err := filepath.Abs(path)
	bailOnError(err)
	relPath, err := filepath.Rel(cwd, absPath)
	bailOnError(err)
	return relPath
}

func populateTaskModTimes(task *RunnableTask) {
	if task.taskDeclaration == nil {
		return
	}
	if task.taskDeclaration.In != nil {
		stat, err := os.Stat(*task.taskDeclaration.In)
		if err == nil {
			task.inTime = stat.ModTime().Unix()
		}
	}
	if task.taskDeclaration.Out != nil {
		stat, err := os.Stat(*task.taskDeclaration.Out)
		if err == nil {
			task.outTime = stat.ModTime().Unix()
		}
	}
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

			if inSha256Value, ok := runnableData["in.sha256"]; ok {
				inSha256String := inSha256Value.(string)
				task.taskDeclaration.InSha256 = &inSha256String
			}

			if outSha256Value, ok := runnableData["out.sha256"]; ok {
				outSha256String := outSha256Value.(string)
				task.taskDeclaration.OutSha256 = &outSha256String
			}

			if runnableValue, ok := runnableData["run"]; ok {
				runString := runnableValue.(string)
				task.taskDeclaration.Run = runString
			} else {
				log.Fatalf("error: %v", "run is required")
			}
			populateTaskModTimes(&task)

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

	if len(os.Args) > 1 {
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
}

func reformatFlowDefinitionFile(flowDefinitionFile string) string {

	var node yaml.Node

	flowDefinitionFileSource, err := os.ReadFile(flowDefinitionFile)
	bailOnError(err)

	originalIndentationLevel := detectFirstIndentationLevel(string(flowDefinitionFileSource))

	if err := yaml.Unmarshal(flowDefinitionFileSource, &node); err != nil {
		log.Fatalf("Unmarshalling failed %s", err)
	}

	outputBuffer := &bytes.Buffer{}
	yamlEncoder := yaml.NewEncoder(outputBuffer)
	yamlEncoder.SetIndent(originalIndentationLevel)

	if err := yamlEncoder.Encode(node.Content[0]); err != nil {
		log.Fatalf("Marshalling failed %s", err)
	}
	yamlEncoder.Close()

	updatedYaml := string(outputBuffer.String())
	return addInterveningSpacesToRootLevelBlocks(updatedYaml)
}

func main() {
	// sample3()
	sample4()

	COLORIZED_PROGRAM_NAME := color.HiBlueString(os.Args[0])

	var rootCmd = &cobra.Command{

		Use: strings.Join(
			[]string{
				fmt.Sprintf("\n- <data source> | %s %s  # (read from STDIN)", COLORIZED_PROGRAM_NAME, color.CyanString("[transformer]")),
				fmt.Sprintf("\n- %s %s", COLORIZED_PROGRAM_NAME, color.CyanString("[input-data] [transformer]")),
				fmt.Sprintf("\n- %s %s", COLORIZED_PROGRAM_NAME, color.CyanString("[flags]")),
				"\n",
				"\n[input-data]  is the path to the input data file to be processed, or - to read from STDIN, or implied as STDIN",
				"\n[transformer] is the path to the jsonata or jsonnet file to be used for transformation, or the code as a string",
				"\n[flags]       specify arguments explicitly for more complex processing; see help",
			},
			"",
		),
		Short: "App transforms JSONL/XSV files based on transformation code.",
		RunE: func(cmd *cobra.Command, args []string) error {

			validateDefintionFileFlag, _ := cmd.Flags().GetBool("reformat")
			if validateDefintionFileFlag {
				validateFlowDefinitionFile(FLOW_DEFINITION_FILE)
			}

			reformatDefinitionFileFlag, _ := cmd.Flags().GetBool("reformat")
			if reformatDefinitionFileFlag {
				reformattedFlowDefinition := reformatFlowDefinitionFile(FLOW_DEFINITION_FILE)
				fmt.Println(reformattedFlowDefinition)
			}

			generateCompletionsFlag, _ := cmd.Flags().GetString("completions")
			if generateCompletionsFlag != "" {
				switch generateCompletionsFlag {
				case "bash":
				case "zsh":
				}
			}

			// if len(args) == 0 {
			// 	return fmt.Errorf("need at least a transformer to do anything")
			// }

			return nil
		},
	}

	rootCmd.Flags().Bool("validate", false, "asdf")
	rootCmd.Flags().Bool("reformat", false, "asdf")
	rootCmd.Flags().String("completions", "", "asdf")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
