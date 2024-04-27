package main

import (
	"bytes"
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

// declare list of candidates for the flow definition file
var FLOW_DEFINITION_FILE_CANDIDATES = []string{
	"Sdflow.yaml",
	"sdflow.yaml",
}
var FLOW_DEFINITION_FILE string
var CACHE_DIRECTORY string = ".sdflow.cache"

//go:embed schemas/Sdflow.yaml.schema.json
var sdflowSpecFileSchema embed.FS

const sdflowSpecFileSchemaPath = "schemas/Sdflow.yaml.schema.json"

//go:embed resources/bash_autocomplete.sh
var bashAutoCompleteScript embed.FS

const bashAutoCompleteScriptPath = "resources/bash_autocomplete.sh"

//go:embed resources/zsh_autocomplete.sh
var zshAutoCompleteScript embed.FS

const zshAutoCompleteScriptPath = "resources/zsh_autocomplete.sh"

type RunnableTaskInput struct {
	path string
	// sha256 string
	mtime int64
}

type RunnableTask struct {
	taskDeclaration  *RunnableSchemaJson
	taskDependencies []*RunnableTask
	targetKey        string // the original key
	targetName       string
	outTime          int64
	inputs           []*RunnableTaskInput
}

func discoverFlowDefinitionFile() string {
	for _, candidate := range FLOW_DEFINITION_FILE_CANDIDATES {
		if _, err := os.Stat(candidate); err == nil {
			return candidate
		}
	}
	log.Fatal("No Sdflow.yaml found")
	return ""
}

func readResourceFile(embedFile embed.FS, name string) []byte {
	resourceBytes, err := fs.ReadFile(embedFile, name)
	bailOnError(err)
	return resourceBytes
}

func validateFlowDefinitionFile(flowDefinitionFile string) {
	var flowDefinitionObject map[string]interface{}
	flowDefinitionSource, err := os.ReadFile(FLOW_DEFINITION_FILE)
	bailOnError(err)
	if err := yaml.Unmarshal([]byte(flowDefinitionSource), &flowDefinitionObject); err != nil {
		log.Fatalf("FAILED TO READ YAML\nerror: %v", err)
	}
	validatorSchemaSource := readResourceFile(sdflowSpecFileSchema, sdflowSpecFileSchemaPath)
	validator := jsonschema.MustCompileString(sdflowSpecFileSchemaPath, string(validatorSchemaSource))
	if err := validator.Validate(flowDefinitionObject); err != nil {
		log.Fatalf("SDFLOW YAML FAILED TO VALIDATE\nerror: %v", err)
	}
}

func prettyPrintTask(task *RunnableTask) {
	fmt.Println("Task:", task.targetName)
	if task.taskDeclaration == nil {
		fmt.Println("  No task declaration found")
		return
	}
	if task.taskDeclaration.In != nil {
		fmt.Println("  In:", task.taskDeclaration.In)
	}
	if task.taskDeclaration.Out != nil {
		fmt.Println("  Out:", *task.taskDeclaration.Out)
	}
	if task.taskDeclaration.Run != nil {
		fmt.Println("  Run:", *task.taskDeclaration.Run)
	}
	if task.taskDeclaration.InSha256 != nil {
		fmt.Println("  In SHA256:", *task.taskDeclaration.InSha256)
	}
	if task.taskDeclaration.OutSha256 != nil {
		fmt.Println("  Out SHA256:", *task.taskDeclaration.OutSha256)
	}
}

func checkIfOutputMoreRecentThanInputs(task *RunnableTask) bool {
	if task.taskDeclaration.Out == nil {
		return false
	}
	isOutputMoreRecent := true
	for _, taskInput := range task.inputs {
		if _, err := os.Stat(taskInput.path); err == nil {
			stat, err := os.Stat(taskInput.path)
			if err == nil {
				taskInput.mtime = stat.ModTime().Unix()
				if task.outTime <= taskInput.mtime {
					isOutputMoreRecent = false
				}
			}
		}
	}
	return isOutputMoreRecent
}

func printVitalsForTask(task *RunnableTask) {
	if task.taskDeclaration == nil {
		return
	}

	var upToDateString string
	if checkIfOutputMoreRecentThanInputs(task) {
		upToDateString = color.GreenString("current")
	} else if task.taskDeclaration.Out == nil {
		upToDateString = color.MagentaString("always ")
	} else {
		upToDateString = color.RedString("stale  ")
	}

	fmt.Fprintf(os.Stderr,
		"%s [%s] (%d)\n  %s --> ",
		upToDateString,
		color.HiWhiteString("%s", task.targetName),
		len(task.taskDependencies),
		color.YellowString("%s", strings.Join(
			func() []string {
				var out []string
				for _, taskInput := range task.inputs {
					out = append(out, taskInput.path)
				}
				return out
			}(), "\n  ")),
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

func renderCommand(task *RunnableTask) string {

	vars := map[string]string{}

	if task.taskDeclaration.In != nil {
		vars["in"] = strings.Join(
			func() []string {
				var out []string
				for _, taskInput := range task.inputs {
					out = append(out, taskInput.path)
				}
				return out
			}(), " ",
		)
		// generate mappings for ${in[0]}, ${in[1]}, etc
		for i, taskInput := range task.inputs {
			vars[fmt.Sprintf("in[%d]", i)] = taskInput.path
		}
	}
	if task.taskDeclaration.Out != nil {
		vars["out"] = *task.taskDeclaration.Out
	}

	mapper := func(varName string) string {
		// check if varName is in vars
		if vars[varName] != "" {
			return vars[varName]
		} else {
			return os.Getenv(varName)
		}
	}

	renderedCommand := os.Expand(*task.taskDeclaration.Run, mapper)
	return renderedCommand
}

func getTaskInputCachePath(task *RunnableTask) string {
	if task.taskDeclaration.InSha256 != nil && len(task.inputs) == 1 {
		return filepath.Join(CACHE_DIRECTORY, *task.taskDeclaration.InSha256)
	}
	return ""
}

func isTaskInputInCache(task *RunnableTask) bool {
	if task.taskDeclaration.InSha256 != nil && len(task.inputs) == 1 {
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

	if len(task.inputs) != 1 {
		panic("saveTaskInputToCache: len(task.inputs) != 1")
	}

	remoteBytes := getRemoteResourceBytes(task.inputs[0].path)
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

func runTask(task *RunnableTask, env map[string]string, shouldUpdateOutSha256 bool) {
	fmt.Fprintf(
		os.Stderr,
		"Running task: %+v (%d dependencies)\n", task.targetName, len(task.taskDependencies))
	printVitalsForTask((task))
	if checkIfOutputMoreRecentThanInputs(task) {
		fmt.Println("Output is up to date")
		return
	}

	for _, dep := range task.taskDependencies {
		fmt.Println("Running dependency:", dep.targetName)
		runTask(dep, env, shouldUpdateOutSha256)
	}

	if task.taskDeclaration == nil {
		fmt.Println(color.RedString("No task declaration found!!!"))
		return
	}

	var shouldCheckOutput bool = false
	if task.taskDeclaration.In != nil && len(task.inputs) == 1 {
		if task.taskDeclaration.InSha256 != nil {
			fmt.Println("Checking sha256 of input file")

			if isRemotePath(task.inputs[0].path) {
				if isTaskInputInCache(task) {
					cachedInputPath := getTaskInputCachePath(task)
					trace(fmt.Sprintf("Using cached input %s", cachedInputPath))
					return
				} else {
					cachedInputPath := saveTaskInputToCache(task)
					trace(fmt.Sprintf("saved input to cache %s", cachedInputPath))
					return
				}
			} else {
				if isFileBytesMatchingSha256(task.inputs[0].path, *task.taskDeclaration.InSha256) {
					trace("IN SHA256 matches!")
				} else {
					trace("IN SHA256 mismatch!")
				}
			}
		} else if task.taskDeclaration.Out != nil && task.taskDeclaration.Run == nil {
			fmt.Println("using built-in downloaders")
			shouldDownloadFile := false
			if isRemotePath(task.inputs[0].path) {
				if _, err := os.Stat(*task.taskDeclaration.Out); err == nil {
					if !isFileBytesMatchingSha256(*task.taskDeclaration.Out, *task.taskDeclaration.OutSha256) {
						fmt.Fprintf(os.Stderr, "warning: SHA256 mismatch for:\n%s; overwriting file", *task.taskDeclaration.Out)
						shouldDownloadFile = true
					}
				} else {
					shouldDownloadFile = true
				}

				if shouldDownloadFile {
					downloadFileToLocalPath(task.inputs[0].path, *task.taskDeclaration.Out)
					shouldCheckOutput = true
				}
			}
		}
	}

	if task.taskDeclaration.Run != nil {
		command := renderCommand(task)
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
		shouldCheckOutput = true
	}

	if shouldUpdateOutSha256 && task.taskDeclaration.Out != nil {
		outputFileBytes, err := os.ReadFile(*task.taskDeclaration.Out)
		bailOnError(err)
		outputSha256 := getBytesSha256(outputFileBytes)
		task.taskDeclaration.OutSha256 = &outputSha256
		trace(fmt.Sprintf("Updated OUT SHA256: %s", outputSha256))
	} else if shouldCheckOutput && task.taskDeclaration.OutSha256 != nil {
		if isFileBytesMatchingSha256(*task.taskDeclaration.Out, *task.taskDeclaration.OutSha256) {
			trace("OUT SHA256 matches!")
		} else {
			trace("OUT SHA256 mismatch!")
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
		for _, taskInput := range task.inputs {
			stat, err := os.Stat(taskInput.path)
			if err == nil {
				taskInput.mtime = stat.ModTime().Unix()
			}
		}
	}
	if task.taskDeclaration.Out != nil {
		stat, err := os.Stat(*task.taskDeclaration.Out)
		if err == nil {
			task.outTime = stat.ModTime().Unix()
		}
	}
}

type ParsedFlowDefinition struct {
	taskLookup       map[string]*RunnableTask
	taskDependencies map[string][]string
	executionEnv     map[string]string
}

func parseFlowDefinitionFile(flowDefinitionFilePath string) *ParsedFlowDefinition {

	taskLookup := make(map[string]*RunnableTask)
	taskDependencies := make(map[string][]string)
	executionEnv := make(map[string]string)

	// add keyvals from environ to executionEnv
	for _, envVar := range os.Environ() {
		parts := strings.Split(envVar, "=")
		executionEnv[parts[0]] = parts[1]
	}

	var flowDefinitionObject map[string]interface{}
	flowDefinitionSource, err := os.ReadFile(flowDefinitionFilePath)
	bailOnError(err)
	if err := yaml.Unmarshal([]byte(flowDefinitionSource), &flowDefinitionObject); err != nil {
		log.Fatalf("error: %v", err)
	}

	// first pass: compile the execution environment
	for targetIdentifier, value := range flowDefinitionObject {

		switch value.(type) {
		case string: // variable definitions
			executionEnv[targetIdentifier] = flowDefinitionObject[targetIdentifier].(string)
		}
	}

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
				taskDependencies[substitutedTargetName] = append(
					taskDependencies[substitutedTargetName],
					*substituteWithContext(subTarget.(string), executionEnv))
			}
			task := RunnableTask{
				targetKey:  targetIdentifier,
				targetName: substitutedTargetName,
			}
			taskLookup[substitutedTargetName] = &task

		default: // all other cases should be map
			runnableData := ruleContent.(map[string]interface{})

			task := RunnableTask{
				targetKey:       targetIdentifier,
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
					taskLookup[fileAbsPath] = &task
				}
			}

			if inValue, ok := runnableData["in"]; ok {
				task.inputs = make([]*RunnableTaskInput, 0)
				if inArray, ok := inValue.([]interface{}); ok {
					// array of input target names / files
					var inputStrings []string
					for _, inItem := range inArray {
						inString := getPathRelativeToCwd(
							*substituteWithContext(inItem.(string), executionEnv))
						inputStrings = append(inputStrings, fmt.Sprintf("\"%s\"", inString))
						task.inputs = append(task.inputs, &RunnableTaskInput{
							path: inString,
						})
						taskDependencies[substitutedTargetName] = append(
							taskDependencies[substitutedTargetName], inString)
					}
					concatenatedInputs := strings.Join(inputStrings, " ")
					// FIXME: probably redundant since we're using task.inputs
					task.taskDeclaration.In = &concatenatedInputs
				} else {
					// assume string
					inString := getPathRelativeToCwd(
						*substituteWithContext(inValue.(string), executionEnv))
					task.taskDeclaration.In = &inString
					task.inputs = append(task.inputs, &RunnableTaskInput{
						path: inString,
					})
					taskDependencies[substitutedTargetName] = append(
						taskDependencies[substitutedTargetName], inString)
				}
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
				task.taskDeclaration.Run = &runString
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
			if depTask != nil {
				if depTask.taskDeclaration == nil {
					bailOnError(fmt.Errorf("subtask %s has no definition!?", dep))
				}
				task.taskDependencies = append(task.taskDependencies, depTask)
			}
		}
	}

	parsedFlowDefinition := ParsedFlowDefinition{
		taskLookup:       taskLookup,
		taskDependencies: taskDependencies,
		executionEnv:     make(map[string]string),
	}
	return &parsedFlowDefinition
}

func runFlowDefinitionProcessor(flowDefinitionFilePath string, shouldWriteOutSha256 bool) {

	parsedFlowDefinition := parseFlowDefinitionFile(flowDefinitionFilePath)

	if len(os.Args) == 1 {
		for targetIdentifier := range parsedFlowDefinition.taskDependencies {
			task := parsedFlowDefinition.taskLookup[targetIdentifier]
			printVitalsForTask(task)
		}
	} else if len(os.Args) > 1 {
		lastArg := os.Args[len(os.Args)-1]
		// see if lastarg is in our lookup
		if _, ok := parsedFlowDefinition.taskLookup[lastArg]; !ok {
			fmt.Printf("Task %s not found\n", lastArg)
			return
		} else {
			task := parsedFlowDefinition.taskLookup[lastArg]
			runTask(task, parsedFlowDefinition.executionEnv, shouldWriteOutSha256)

			if shouldWriteOutSha256 && task.taskDeclaration.OutSha256 != nil {
				updatedYamlString := updateOutSha256ForTarget(FLOW_DEFINITION_FILE, task.targetKey, *task.taskDeclaration.OutSha256)
				outputYamlString := addInterveningSpacesToRootLevelBlocks(updatedYamlString)
				// re-output the file
				currentFileMode := os.ModePerm
				if fileInfo, err := os.Stat(FLOW_DEFINITION_FILE); err == nil {
					currentFileMode = fileInfo.Mode()
				}
				flowDefinitionSource, err := os.ReadFile(FLOW_DEFINITION_FILE)
				bailOnError(err)
				err = os.WriteFile(FLOW_DEFINITION_FILE, []byte(outputYamlString), currentFileMode)
				if err != nil {
					log.Printf("original file: %s", flowDefinitionSource)
					log.Fatalf("error: %v", err)
				}
			}
		}
	}
}

func reformatFlowDefinitionFile(flowDefinitionFile string) string {
	flowDefinitionFileSource, err := os.ReadFile(flowDefinitionFile)
	bailOnError(err)
	originalIndentationLevel := detectFirstIndentationLevel(string(flowDefinitionFileSource))

	var node yaml.Node
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

	COLORIZED_PROGRAM_NAME := color.HiBlueString(os.Args[0])
	FLOW_DEFINITION_FILE = discoverFlowDefinitionFile()
	if os.Getenv("SDFLOW_CACHE_DIRECTORY") != "" {
		CACHE_DIRECTORY = os.Getenv("SDFLOW_CACHE_DIRECTORY")
	}

	var rootCmd = &cobra.Command{

		Use: strings.Join(
			[]string{
				fmt.Sprintf("\n- %s %s", COLORIZED_PROGRAM_NAME, color.CyanString("[flags]")),
				"\n",
				"\n[validate]  is the path to the input data file to be processed, or - to read from STDIN, or implied as STDIN",
				"\n[completions] is the path to the jsonata or jsonnet file to be used for transformation, or the code as a string",
			},
			"",
		),
		Short: "flow runner",
		RunE: func(cmd *cobra.Command, args []string) error {

			targetsFlag, _ := cmd.Flags().GetBool("targets")
			if targetsFlag {
				parsedFlowDefinition := parseFlowDefinitionFile(FLOW_DEFINITION_FILE)
				for _, task := range parsedFlowDefinition.taskLookup {
					fmt.Fprintf(os.Stdout, "%s\n", task.targetName)
				}
				return nil
			}

			validateDefintionFileFlag, _ := cmd.Flags().GetBool("validate")
			if validateDefintionFileFlag {
				validateFlowDefinitionFile(FLOW_DEFINITION_FILE)
				reformattedFlowDefinition := reformatFlowDefinitionFile(FLOW_DEFINITION_FILE)
				fmt.Println(reformattedFlowDefinition)
				return nil
			}

			generateCompletionsFlag, _ := cmd.Flags().GetString("completions")
			if generateCompletionsFlag != "" {
				switch generateCompletionsFlag {
				case "bash":
					fmt.Println(string(readResourceFile(bashAutoCompleteScript, bashAutoCompleteScriptPath)))
					return nil

				case "zsh":
					fmt.Println(string(readResourceFile(zshAutoCompleteScript, zshAutoCompleteScriptPath)))
					return nil

				default:
					return fmt.Errorf("unsupported shell type: %s", generateCompletionsFlag)
				}
			}

			shouldUpdateOutSha256, _ := cmd.Flags().GetBool("updatehash")

			runFlowDefinitionProcessor(FLOW_DEFINITION_FILE, shouldUpdateOutSha256)
			return nil
		},
	}

	rootCmd.Flags().Bool("validate", false, "validate the flow definition file")
	rootCmd.Flags().String("completions", "", "get shell completion code for the given shell type")
	rootCmd.Flags().Bool("updatehash", false, "update out.sha256 for the target in the flow definition file after running the target")
	rootCmd.Flags().Bool("targets", false, "list all defined targets")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
