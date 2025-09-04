package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"embed"

	"github.com/spf13/cobra"

	"github.com/fatih/color"
	"github.com/santhosh-tekuri/jsonschema/v5"
	yaml "gopkg.in/yaml.v3"
)

// Executor interface defines execution strategies
type Executor interface {
	// Core execution methods
	ExecuteCommand(task *RunnableTask, command string, env []string) error
	DownloadFile(url, outputPath string) error
	ShouldUpdateSha256() bool
	ShouldForceRun() bool

	// Output methods for different execution phases
	ShowTaskStart(task *RunnableTask)
	ShowTaskSkip(task *RunnableTask, reason string)
	ShowTaskCompleted(task *RunnableTask)
}

// RealExecutor performs actual execution
type RealExecutor struct {
	updateSha256 bool
	forceRun     bool
}

func NewRealExecutor(updateSha256, forceRun bool) *RealExecutor {
	return &RealExecutor{updateSha256: updateSha256, forceRun: forceRun}
}

func (e *RealExecutor) ExecuteCommand(task *RunnableTask, command string, env []string) error {
	fmt.Fprint(
		os.Stderr,
		color.GreenString("Command: %s\n", command),
	)

	cmd := exec.Command("bash", "-c", command)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = env

	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("error executing command: %v", err)
	}
	return nil
}

func (e *RealExecutor) DownloadFile(url, outputPath string) error {
	return downloadFileToLocalPath(url, outputPath)
}

func (e *RealExecutor) ShouldUpdateSha256() bool {
	return e.updateSha256
}

func (e *RealExecutor) ShouldForceRun() bool {
	return e.forceRun
}

func (e *RealExecutor) ShowTaskStart(task *RunnableTask) {
	fmt.Fprintf(
		os.Stderr,
		"Running task: %+v (%d dependencies)\n", task.targetName, len(task.taskDependencies))
	printVitalsForTask(task)
}

func (e *RealExecutor) ShowTaskSkip(task *RunnableTask, reason string) {
	fmt.Printf("Output is up to date\n")
}

func (e *RealExecutor) ShowTaskCompleted(task *RunnableTask) {
}

// DryRunExecutor simulates execution with pretty output
type DryRunExecutor struct {
	updateSha256 bool
	forceRun     bool
}

func NewDryRunExecutor(updateSha256, forceRun bool) *DryRunExecutor {
	return &DryRunExecutor{updateSha256: updateSha256, forceRun: forceRun}
}

func (e *DryRunExecutor) ExecuteCommand(task *RunnableTask, command string, env []string) error {
	fmt.Fprintf(os.Stderr, "%s [DRY RUN WOULD EXECUTE]: %s\n", task.targetName, command)
	return nil
}

func (e *DryRunExecutor) DownloadFile(url, outputPath string) error {
	fmt.Fprintf(os.Stderr, "[DRY RUN WOULD DOWNLOAD]: %s -> %s\n", url, outputPath)
	return nil
}

func (e *DryRunExecutor) ShouldUpdateSha256() bool {
	return e.updateSha256
}

func (e *DryRunExecutor) ShouldForceRun() bool {
	return e.forceRun
}

func (e *DryRunExecutor) ShowTaskStart(task *RunnableTask) {
	fmt.Fprintf(
		os.Stderr,
		"Running task: %+v (%d dependencies)\n", task.targetName, len(task.taskDependencies))
	printVitalsForTask(task)
}

func (e *DryRunExecutor) ShowTaskSkip(task *RunnableTask, reason string) {
	fmt.Fprintf(os.Stderr, "%s [DRY RUN SKIPPED] (%s)\n", task.targetName, reason)
}

func (e *DryRunExecutor) ShowTaskCompleted(task *RunnableTask) {
}

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

type TaskExecutionState int

const (
	TaskNotStarted TaskExecutionState = iota
	TaskCompleted
	TaskSkipped // up-to-date
)

type RunnableTaskInput struct {
	path   string
	sha256 string
	mtime  int64
	alias  string // New field for named inputs
}

type RunnableTask struct {
	taskDeclaration  *RunnableSchemaJson
	taskDependencies []*RunnableTask
	targetKey        string // the original key
	targetName       string
	outTime          int64
	inputs           []*RunnableTaskInput
	executionState   TaskExecutionState
	executionCount   int // for testing
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
		fmt.Println("  In SHA256:", task.taskDeclaration.InSha256)
	}
	if task.taskDeclaration.OutSha256 != nil {
		fmt.Println("  Out SHA256:", *task.taskDeclaration.OutSha256)
	}
}

func checkIfOutputMoreRecentThanInputs(task *RunnableTask) bool {
	if task.taskDeclaration == nil || task.taskDeclaration.Out == nil {
		return false
	}

	// Check if output file exists
	outputStat, err := os.Stat(*task.taskDeclaration.Out)
	if err != nil {
		// Output file doesn't exist, so task needs to run
		return false
	}

	outputTime := outputStat.ModTime().Unix()

	// Check all inputs - if any input is newer than output, task needs to run
	for _, taskInput := range task.inputs {
		if inputStat, err := os.Stat(taskInput.path); err == nil {
			taskInput.mtime = inputStat.ModTime().Unix()
			if outputTime <= taskInput.mtime {
				return false // Input is newer than output
			}
		}
		// If input doesn't exist, we can't compare - assume task needs to run
		// (This handles cases where inputs are produced by other tasks)
	}

	return true // Output exists and is newer than all existing inputs
}

func printVitalsForTask(task *RunnableTask) {
	if task.taskDeclaration == nil {
		return
	}

	var upToDateString string
	var coloringFunc func(format string, a ...interface{}) string
	if checkIfOutputMoreRecentThanInputs(task) {
		upToDateString = color.GreenString("current")
		coloringFunc = color.HiGreenString
	} else if task.taskDeclaration.Out == nil {
		upToDateString = color.MagentaString("always")
		coloringFunc = color.HiMagentaString
	} else {
		upToDateString = color.RedString("stale")
		coloringFunc = color.HiRedString
	}

	fmt.Fprintf(os.Stderr,
		"╭─❮❮ %s ❯❯\n",
		coloringFunc("%s", task.targetName),
	)

	for _, taskInput := range task.inputs {
		fmt.Fprintf(os.Stderr,
			"├ %s\n",
			color.WhiteString("%s", normalizePathForDisplay(taskInput.path)),
		)
	}

	fmt.Fprintf(os.Stderr, "╰─▶ ")
	if task.taskDeclaration.Out == nil {
		fmt.Fprint(
			os.Stderr,
			color.CyanString("%s", "<STDOUT>"),
		)
	} else {
		fmt.Fprintf(os.Stderr,
			"%s",
			color.HiBlueString("%s", normalizePathForDisplay(*task.taskDeclaration.Out)),
		)
	}
	fmt.Fprintf(os.Stderr, " (%s)", upToDateString)

	fmt.Fprintf(os.Stderr, "\n\n")
}

func substituteWithContext(s string, context map[string]string) *string {
	mapper := func(varName string) string {
		return context[varName]
	}

	substituted := os.Expand(s, mapper)
	return &substituted
}

func renderCommand(task *RunnableTask, env map[string][]string) string {
	// Create a combined environment map that includes task-specific variables
	combinedEnv := make(map[string][]string)
	for k, v := range env {
		combinedEnv[k] = v
	}

	// Add task-specific variables
	if task.taskDeclaration.Out != nil {
		combinedEnv["out"] = []string{*task.taskDeclaration.Out}
	}

	// Add input variables
	var inPaths []string
	for i, input := range task.inputs {
		inPaths = append(inPaths, input.path)

		// Add input alias variables like "in.first", "in.second"
		if input.alias != "" {
			aliasKey := fmt.Sprintf("in.%s", input.alias)
			combinedEnv[aliasKey] = []string{input.path}
		}

		// Add indexed input variables like "in_0", "in_1" for ${in[0]}, ${in[1]}
		indexKey := fmt.Sprintf("in_%d", i)
		combinedEnv[indexKey] = []string{input.path}
	}
	combinedEnv["in"] = inPaths

	if task.taskDeclaration.Run != nil {
		fmt.Printf("Run command: %s\n", *task.taskDeclaration.Run)
	}

	renderedCommand := expandVariables(*task.taskDeclaration.Run, combinedEnv)
	return renderedCommand
}

func getTaskInputCachePath(task *RunnableTask, inputPath string) (string, bool) {
	if task.taskDeclaration.InSha256 == nil {
		return "", false
	}

	switch sha256Value := task.taskDeclaration.InSha256.(type) {
	case string:
		// For single input with string SHA256
		if len(task.inputs) != 1 {
			return "", false
		}
		return filepath.Join(CACHE_DIRECTORY, sha256Value), true

	case map[string]interface{}:
		// For map SHA256, find the matching SHA256 for this input path
		var sha256 string
		var ok bool

		// Try to match by alias first
		for _, input := range task.inputs {
			if input.path == inputPath && input.alias != "" {
				sha256, ok = sha256Value[input.alias].(string)
				if ok {
					break
				}
			}
		}

		// If no alias match, try direct path
		if !ok {
			sha256, ok = sha256Value[inputPath].(string)
		}

		if !ok {
			return "", false
		}

		return filepath.Join(CACHE_DIRECTORY, sha256), true

	default:
		return "", false
	}
}

func isTaskInputInCache(task *RunnableTask, inputPath string) bool {
	cachePath, ok := getTaskInputCachePath(task, inputPath)
	if !ok {
		return false
	}
	_, err := os.Stat(cachePath)
	return err == nil
}

func saveTaskInputToCache(task *RunnableTask) string {
	if _, err := os.Stat(CACHE_DIRECTORY); os.IsNotExist(err) {
		os.Mkdir(CACHE_DIRECTORY, 0755)
	}

	if len(task.inputs) == 0 {
		return ""
	}

	switch sha256Value := task.taskDeclaration.InSha256.(type) {
	case string:
		// For single input with string SHA256
		if len(task.inputs) != 1 {
			return ""
		}
		remoteBytes := getRemoteResourceBytes(task.inputs[0].path)
		fmt.Println("Downloaded content length:", len(remoteBytes))
		if isBytesMatchingSha256(remoteBytes, sha256Value) {
			fmt.Printf("SHA256 matches for %s\n", task.inputs[0].path)
		} else {
			fmt.Printf("SHA256 mismatch for %s\n", task.inputs[0].path)
			return ""
		}
		cachePath := filepath.Join(CACHE_DIRECTORY, sha256Value)
		err := os.WriteFile(cachePath, remoteBytes, 0644)
		bailOnError(err)
		return cachePath

	case map[string]interface{}:
		// For multiple inputs with map SHA256
		var lastCachePath string
		for _, input := range task.inputs {
			// Try to match by alias first
			var sha256 string
			var ok bool
			if input.alias != "" {
				sha256, ok = sha256Value[input.alias].(string)
			}
			if !ok {
				// Try to match by path
				sha256, ok = sha256Value[input.path].(string)
			}
			if !ok {
				continue // Skip this input if no matching SHA256 found
			}

			remoteBytes := getRemoteResourceBytes(input.path)
			fmt.Printf("Downloaded content length for %s: %d\n", input.path, len(remoteBytes))
			if isBytesMatchingSha256(remoteBytes, sha256) {
				fmt.Printf("SHA256 matches for %s!\n", input.path)
			} else {
				fmt.Printf("SHA256 mismatch for %s!\n", input.path)
				continue
			}

			cachePath := filepath.Join(CACHE_DIRECTORY, sha256)
			err := os.WriteFile(cachePath, remoteBytes, 0644)
			bailOnError(err)
			lastCachePath = cachePath
		}
		return lastCachePath

	default:
		return ""
	}
}

func handleRemoteInput(task *RunnableTask, input *RunnableTaskInput) bool {
	if !isRemotePath(input.path) {
		return false
	}

	if task.taskDeclaration.InSha256 == nil {
		return false
	}

	// Convert string SHA256 to a single-entry map for consistent handling
	var sha256Map map[string]interface{}
	switch sha256Value := task.taskDeclaration.InSha256.(type) {
	case string:
		// For string SHA256, create a single-entry map with the input path as key
		sha256Map = map[string]interface{}{
			input.path: sha256Value,
		}
	case map[string]interface{}:
		sha256Map = sha256Value
	default:
		return false
	}

	// Try to match by alias first
	var ok bool
	if input.alias != "" {
		_, ok = sha256Map[input.alias].(string)
	}
	if !ok {
		// Try to match by path
		_, ok = sha256Map[input.path].(string)
	}
	if !ok {
		return false
	}

	if isTaskInputInCache(task, input.path) {
		cachedInputPath, _ := getTaskInputCachePath(task, input.path)
		trace(fmt.Sprintf("Using cached input %s", cachedInputPath))
		return true
	} else {
		cachedInputPath := saveTaskInputToCache(task)
		trace(fmt.Sprintf("saved input to cache %s", cachedInputPath))
		return true
	}
}

func runTask(task *RunnableTask, env map[string][]string, executor Executor) {

	// Check execution state first - prevent duplicate execution
	if task.executionState == TaskCompleted {
		return
	}

	// Check if up-to-date (existing logic)
	if !executor.ShouldForceRun() && checkIfOutputMoreRecentThanInputs(task) {
		task.executionState = TaskSkipped
		executor.ShowTaskSkip(task, "up-to-date")
		return
	}

	executor.ShowTaskStart(task)

	for _, dep := range task.taskDependencies {
		fmt.Println("Running dependency:", dep.targetName)
		runTask(dep, env, executor)
	}

	if task.taskDeclaration == nil {
		fmt.Println(color.RedString("No task declaration found!!!"))
		return
	}

	var shouldCheckOutput bool = false
	if task.taskDeclaration.In != nil && len(task.inputs) > 0 {
		if task.taskDeclaration.InSha256 != nil {
			trace("Checking sha256 of input file")

			// Handle each input individually
			for _, input := range task.inputs {
				if handleRemoteInput(task, input) {
					continue
				}
				// Handle local file SHA256 verification
				if input.sha256 != "" && isFileBytesMatchingSha256(input.path, input.sha256) {
					trace(fmt.Sprintf("IN SHA256 matches for %s", input.path))
				} else {
					trace(fmt.Sprintf("IN SHA256 mismatch for %s", input.path))
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
					err := executor.DownloadFile(task.inputs[0].path, *task.taskDeclaration.Out)
					if err != nil {
						fmt.Printf("Error downloading file: %v\n", err)
						return
					}
					shouldCheckOutput = true
				}
			}
		}
	}

	if task.taskDeclaration.Run != nil {
		command := renderCommand(task, env)

		var cmdEnv []string
		cmdEnv = append(cmdEnv, os.Environ()...)
		for key, value := range env {
			cmdEnv = append(cmdEnv, fmt.Sprintf("%s=%s", key, strings.Join(value, " ")))
		}

		err := executor.ExecuteCommand(task, command, cmdEnv)
		if err != nil {
			fmt.Printf("Error executing command: %v\n", err)
			return
		}
		shouldCheckOutput = true
	}

	if executor.ShouldUpdateSha256() && task.taskDeclaration.Out != nil {
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

	// Mark task as completed and increment execution count
	task.executionState = TaskCompleted
	task.executionCount++
	executor.ShowTaskCompleted(task)
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
	if !strings.HasPrefix(relPath, ".") {
		relPath = "./" + relPath
	}
	return relPath
}

// normalizePathForDisplay ensures consistent "./" prefix for relative local paths
func normalizePathForDisplay(path string) string {
	// Don't modify URLs, absolute paths, or paths that already have proper prefixes
	if isRemotePath(path) || strings.HasPrefix(path, "/") || strings.HasPrefix(path, "./") || strings.HasPrefix(path, "../") {
		return path
	}
	// For simple relative filenames like "main.go", add "./" prefix
	return "./" + path
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
			} else {
				taskInput.mtime = time.Now().Unix()
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
	executionEnv     map[string][]string
}

func createTaskFromRunnableKeyVals(
	targetIdentifier string,
	substitutedTargetName string,
	runnableData map[string]interface{},
	executionEnv map[string][]string,
) *RunnableTask {
	task := RunnableTask{
		targetKey:       targetIdentifier,
		targetName:      substitutedTargetName,
		taskDeclaration: &RunnableSchemaJson{},
	}

	if isPath(task.targetName) {
		fileAbsPath := getPathRelativeToCwd(task.targetName)
		task.taskDeclaration.Out = &fileAbsPath
	} else {
		if outputPathValue, ok := runnableData["out"]; ok {
			pathString := outputPathValue.(string)
			if isPath(pathString) {
				task.taskDeclaration.Out = &pathString
			} else {
				fileAbsPath := getPathRelativeToCwd(
					*substituteWithContext(pathString, convertArrayMapToStringMap(executionEnv)))
				task.taskDeclaration.Out = &fileAbsPath
			}
		}
	}

	if inValue, ok := runnableData["in"]; ok {
		task.inputs = make([]*RunnableTaskInput, 0)
		switch v := inValue.(type) {
		case []interface{}:
			// array of input target names / files
			var inputStrings []string
			for _, inItem := range v {
				itemStr := inItem.(string)
				// Check if this item is an array variable reference that should be expanded
				expandedInputs := expandArrayVariableInInput(itemStr, executionEnv)

				for _, expandedInput := range expandedInputs {
					inString := getPathRelativeToCwd(expandedInput)
					inputStrings = append(inputStrings, fmt.Sprintf("\"%s\"", inString))
					task.inputs = append(task.inputs, &RunnableTaskInput{
						path:  inString,
						alias: inString,
					})
				}
			}
			concatenatedInputs := strings.Join(inputStrings, " ")
			task.taskDeclaration.In = &concatenatedInputs
		case map[string]interface{}:
			// map of alias -> input path
			var inputStrings []string
			for alias, inItem := range v {
				inString := getPathRelativeToCwd(
					*substituteWithContext(inItem.(string), convertArrayMapToStringMap(executionEnv)))
				inputStrings = append(inputStrings, fmt.Sprintf("\"%s\"", inString))
				task.inputs = append(task.inputs, &RunnableTaskInput{
					path:  inString,
					alias: alias,
				})
			}
			concatenatedInputs := strings.Join(inputStrings, " ")
			task.taskDeclaration.In = &concatenatedInputs
		default:
			// assume string - but check if it's an array variable reference
			inputStr := inValue.(string)
			expandedInputs := expandArrayVariableInInput(inputStr, executionEnv)

			var inputStrings []string
			for _, expandedInput := range expandedInputs {
				inString := getPathRelativeToCwd(expandedInput)
				inputStrings = append(inputStrings, fmt.Sprintf("\"%s\"", inString))
				task.inputs = append(task.inputs, &RunnableTaskInput{
					path: inString,
				})
			}

			if len(expandedInputs) == 1 {
				// Single input - use the simple form
				task.taskDeclaration.In = &expandedInputs[0]
			} else {
				// Multiple inputs - use space-separated quoted form
				concatenatedInputs := strings.Join(inputStrings, " ")
				task.taskDeclaration.In = &concatenatedInputs
			}
		}
	}
	if inSha256Value, ok := runnableData["in.sha256"]; ok {
		task.taskDeclaration.InSha256 = inSha256Value

		// Handle map case for per-input SHA256s to populate the input.sha256 field
		if sha256Map, ok := inSha256Value.(map[string]interface{}); ok {
			for _, input := range task.inputs {
				// Try to match by alias first if it exists
				if input.alias != "" {
					if sha256, ok := sha256Map[input.alias].(string); ok {
						input.sha256 = sha256
						continue
					}
				}
				// Try to match by path
				if sha256, ok := sha256Map[input.path].(string); ok {
					input.sha256 = sha256
				}
			}
		} else if sha256String, ok := inSha256Value.(string); ok && len(task.inputs) == 1 {
			// Handle single string case - apply to single input
			task.inputs[0].sha256 = sha256String
		}
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

	return &task
}

func parseFlowDefinitionFile(flowDefinitionFilePath string) *ParsedFlowDefinition {

	flowDefinitionSource, err := os.ReadFile(flowDefinitionFilePath)
	bailOnError(err)

	return parseFlowDefinitionSource(string(flowDefinitionSource))
}

func parseFlowDefinitionSource(flowDefinitionSource string) *ParsedFlowDefinition {

	taskLookup := make(map[string]*RunnableTask)
	taskDependencies := make(map[string][]string)
	executionEnv := convertEnvironToArrayMap(getOsEnvironAsMap())

	var flowDefinitionObject map[string]interface{}

	if err := yaml.Unmarshal([]byte(flowDefinitionSource), &flowDefinitionObject); err != nil {
		log.Fatalf("error: %v", err)
	}

	// first pass: compile the execution environment
	for targetIdentifier, value := range flowDefinitionObject {

		trace(fmt.Sprintf("Processing key: %s, value: %v", targetIdentifier, value))

		switch value.(type) {
		case string: // variable definitions
			trace(fmt.Sprintf("Adding environment variable: %s=%s", targetIdentifier, flowDefinitionObject[targetIdentifier].(string)))
			executionEnv[targetIdentifier] = []string{flowDefinitionObject[targetIdentifier].(string)}
		case []interface{}: // array variable definitions
			var stringArray []string
			for _, item := range value.([]interface{}) {
				if str, ok := item.(string); ok {
					stringArray = append(stringArray, str)
				}
			}
			trace(fmt.Sprintf("Adding array environment variable: %s=%v", targetIdentifier, stringArray))
			executionEnv[targetIdentifier] = stringArray
		}
	}

	/*
		fmt.Fprintf(os.Stderr, "╭─ Environment Variables ─╮\n")
		for key, value := range executionEnv {
			value = strings.ReplaceAll(value, "\n", "↵")
			if len(value) > 80 {
				value = value[:77] + "..."
			}
			fmt.Fprintf(os.Stderr, "│ %s=%s\n", color.HiYellowString(key), color.HiWhiteString(value))
		}
		fmt.Fprintf(os.Stderr, "╰────────────────────────╯\n\n")
		// */

	// second pass: retrieve tasks and substitute using executionEnv
	for targetIdentifier, value := range flowDefinitionObject {

		if _, exists := executionEnv[targetIdentifier]; exists {
			// skip variable definitions
			continue
		}
		substitutedTargetName := *substituteWithContext(targetIdentifier, convertArrayMapToStringMap(executionEnv))

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
					*substituteWithContext(subTarget.(string), convertArrayMapToStringMap(executionEnv)))
			}
			task := RunnableTask{
				targetKey:  targetIdentifier,
				targetName: substitutedTargetName,
			}
			taskLookup[substitutedTargetName] = &task

		default: // all other cases should be map
			runnableData := ruleContent.(map[string]interface{})
			task := createTaskFromRunnableKeyVals(targetIdentifier, substitutedTargetName, runnableData, executionEnv)
			taskLookup[substitutedTargetName] = task
			if task.taskDeclaration.Out != nil {
				taskLookup[*task.taskDeclaration.Out] = task
			}
		}
	}

	// Add implicit dependencies based on input/output file matching
	addImplicitDependencies(taskLookup, taskDependencies)

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
		executionEnv:     executionEnv,
	}
	return &parsedFlowDefinition
}

func addImplicitDependencies(taskLookup map[string]*RunnableTask, taskDependencies map[string][]string) {
	for taskName, task := range taskLookup {
		if task.taskDeclaration == nil {
			continue
		}

		// Only process tasks that are actual task names, not output file aliases
		// Check if this taskName exists in taskDependencies (which only contains real task names)
		if _, isRealTask := taskDependencies[taskName]; !isRealTask {
			continue
		}

		for _, input := range task.inputs {
			// Check if input path matches any task's output file
			if producingTask, exists := taskLookup[input.path]; exists {
				if producingTask.taskDeclaration != nil && producingTask.taskDeclaration.Out != nil &&
					*producingTask.taskDeclaration.Out == input.path {

					// Find the actual task name that produces this output (not the output file alias)
					var producingTaskName string
					for name, t := range taskLookup {
						if t == producingTask && name == producingTask.targetName {
							producingTaskName = name
							break
						}
					}

					if producingTaskName != "" {
						// Check if this dependency already exists to avoid duplicates
						dependencyExists := false
						for _, existingDep := range taskDependencies[taskName] {
							if existingDep == producingTaskName {
								dependencyExists = true
								break
							}
						}

						if !dependencyExists {
							// Add implicit dependency using the task name, not the output file
							taskDependencies[taskName] = append(taskDependencies[taskName], producingTaskName)
						}
					}
				}
			}

			// Also check if input path matches a task name directly (for task references)
			if producingTask, exists := taskLookup[input.path]; exists {
				if producingTask.taskDeclaration != nil && producingTask.targetName == input.path {
					// This is a direct task reference
					producingTaskName := producingTask.targetName

					// Check if this dependency already exists to avoid duplicates
					dependencyExists := false
					for _, existingDep := range taskDependencies[taskName] {
						if existingDep == producingTaskName {
							dependencyExists = true
							break
						}
					}

					if !dependencyExists {
						// Add implicit dependency using the task name
						taskDependencies[taskName] = append(taskDependencies[taskName], producingTaskName)
					}
				}
			}
		}
	}
}

func runFlowDefinitionProcessor(flowDefinitionFilePath string, executor Executor) {

	parsedFlowDefinition := parseFlowDefinitionFile(flowDefinitionFilePath)

	if len(os.Args) == 1 {
		// Collect and sort target identifiers
		var targetIdentifiers []string
		for targetIdentifier := range parsedFlowDefinition.taskDependencies {
			targetIdentifiers = append(targetIdentifiers, targetIdentifier)
		}

		// Sort case-insensitive, trimming leading . and /
		sort.Slice(targetIdentifiers, func(i, j int) bool {
			// Trim leading . and / for comparison
			trimI := strings.TrimLeft(targetIdentifiers[i], "./")
			trimJ := strings.TrimLeft(targetIdentifiers[j], "./")
			return strings.ToLower(trimI) < strings.ToLower(trimJ)
		})

		// Print tasks in sorted order
		for _, targetIdentifier := range targetIdentifiers {
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
			runTask(task, parsedFlowDefinition.executionEnv, executor)

			if executor.ShouldUpdateSha256() && task.taskDeclaration.OutSha256 != nil {
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
			shouldForceRun, _ := cmd.Flags().GetBool("always-run")
			isDryRun, _ := cmd.Flags().GetBool("dry-run")
			// TODO FIXME: tell user that updating the hash is meaningless if `out` is not supplied

			var executor Executor
			if isDryRun {
				executor = NewDryRunExecutor(shouldUpdateOutSha256, shouldForceRun)
			} else {
				executor = NewRealExecutor(shouldUpdateOutSha256, shouldForceRun)
			}

			runFlowDefinitionProcessor(FLOW_DEFINITION_FILE, executor)
			return nil
		},
	}

	// TODO: should support last run timestamp writing to sdflow? -- probably not, use git
	// TODO: should support write to s3 target? what about dynamic name?
	rootCmd.Flags().Bool("validate", false, "validate the flow definition file")
	rootCmd.Flags().String("completions", "", "get shell completion code for the given shell type")
	rootCmd.Flags().Bool("updatehash", false, "update out.sha256 for the target in the flow definition file after running the target")
	rootCmd.Flags().Bool("targets", false, "list all defined targets")
	rootCmd.Flags().BoolP("always-run", "B", false, "always run the target, even if it's up to date")
	rootCmd.Flags().Bool("dry-run", false, "show execution plan without running commands")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
