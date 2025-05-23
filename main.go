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
	"strconv"
	"strings"
	"time"

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
			color.WhiteString("%s", taskInput.path),
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
			color.HiBlueString("%s", *task.taskDeclaration.Out),
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

func renderCommand(task *RunnableTask) string {

	mapper := func(varName string) string {

		// Handle $out
		if varName == "out" && task.taskDeclaration.Out != nil {
			return *task.taskDeclaration.Out
		}

		// Handle $in
		if varName == "in" {
			var paths []string
			for _, input := range task.inputs {
				paths = append(paths, input.path)
			}
			return strings.Join(paths, " ")
		}

		// Handle ${in[N]}
		if strings.HasPrefix(varName, "in[") && strings.HasSuffix(varName, "]") {
			idxStr := varName[3 : len(varName)-1]
			if idx, err := strconv.Atoi(idxStr); err == nil {
				if input := task.getInputByIndex(idx); input != nil {
					return input.path
				}
			}
			fmt.Fprintf(os.Stderr, "!!!! WARN no input found for index: %s\n", idxStr)
		}

		// Handle ${in.foo}
		if strings.HasPrefix(varName, "in.") {
			alias := varName[3:]
			if input := task.getInputByAlias(alias); input != nil {
				return input.path
			}
			fmt.Fprintf(os.Stderr, "!!!! WARN no input found for alias: %s\n", alias)
		}

		// Fall back to environment variables
		return os.Getenv(varName)
	}

	if task.taskDeclaration.Run != nil {
		fmt.Printf("Run command: %s\n", *task.taskDeclaration.Run)
	}

	renderedCommand := os.Expand(*task.taskDeclaration.Run, mapper)
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

func runTask(task *RunnableTask, env map[string]string, shouldUpdateOutSha256 bool, shouldForceRun bool) {
	fmt.Fprintf(
		os.Stderr,
		"Running task: %+v (%d dependencies)\n", task.targetName, len(task.taskDependencies))
	printVitalsForTask((task))

	if !shouldForceRun && checkIfOutputMoreRecentThanInputs(task) {
		fmt.Println("Output is up to date")
		return
	}

	for _, dep := range task.taskDependencies {
		fmt.Println("Running dependency:", dep.targetName)
		runTask(dep, env, shouldUpdateOutSha256, shouldForceRun)
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
	executionEnv     map[string]string
}

func createTaskFromRunnableKeyVals(
	targetIdentifier string,
	substitutedTargetName string,
	runnableData map[string]interface{},
	executionEnv map[string]string,
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
					*substituteWithContext(pathString, executionEnv))
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
				inString := getPathRelativeToCwd(
					*substituteWithContext(inItem.(string), executionEnv))
				inputStrings = append(inputStrings, fmt.Sprintf("\"%s\"", inString))
				task.inputs = append(task.inputs, &RunnableTaskInput{
					path:  inString,
					alias: inString,
				})
			}
			concatenatedInputs := strings.Join(inputStrings, " ")
			task.taskDeclaration.In = &concatenatedInputs
		case map[string]interface{}:
			// map of alias -> input path
			var inputStrings []string
			for alias, inItem := range v {
				inString := getPathRelativeToCwd(
					*substituteWithContext(inItem.(string), executionEnv))
				inputStrings = append(inputStrings, fmt.Sprintf("\"%s\"", inString))
				task.inputs = append(task.inputs, &RunnableTaskInput{
					path:  inString,
					alias: alias,
				})
			}
			concatenatedInputs := strings.Join(inputStrings, " ")
			task.taskDeclaration.In = &concatenatedInputs
		default:
			// assume string
			inString := getPathRelativeToCwd(
				*substituteWithContext(inValue.(string), executionEnv))
			task.taskDeclaration.In = &inString
			task.inputs = append(task.inputs, &RunnableTaskInput{
				path: inString,
			})
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
	executionEnv := getOsEnvironAsMap()

	var flowDefinitionObject map[string]interface{}

	if err := yaml.Unmarshal([]byte(flowDefinitionSource), &flowDefinitionObject); err != nil {
		log.Fatalf("error: %v", err)
	}

	// first pass: compile the execution environment
	for targetIdentifier, value := range flowDefinitionObject {

		trace(fmt.Sprintf("Processing key: %s, value: %v", targetIdentifier, value))

		switch value.(type) {
		case string: // variable definitions
			executionEnv[targetIdentifier] = flowDefinitionObject[targetIdentifier].(string)
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
			task := createTaskFromRunnableKeyVals(targetIdentifier, substitutedTargetName, runnableData, executionEnv)
			taskLookup[substitutedTargetName] = task
			if task.taskDeclaration.Out != nil {
				taskLookup[*task.taskDeclaration.Out] = task
			}
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

func runFlowDefinitionProcessor(flowDefinitionFilePath string, shouldWriteOutSha256 bool, shouldForceRun bool) {

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
			runTask(task, parsedFlowDefinition.executionEnv, shouldWriteOutSha256, shouldForceRun)

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

// For ordered access (indices)
func (task *RunnableTask) getInputByIndex(idx int) *RunnableTaskInput {
	if idx >= 0 && idx < len(task.inputs) {
		return task.inputs[idx]
	}
	return nil
}

// For named access (aliases)
func (task *RunnableTask) getInputByAlias(alias string) *RunnableTaskInput {
	for _, input := range task.inputs {
		if input.alias == alias {
			return input
		}
	}
	return nil
}

func (task *RunnableTask) expandInputReference(ref string) string {
	// Handle $in case - expand all inputs
	if ref == "in" {
		var paths []string
		for _, input := range task.inputs {
			paths = append(paths, input.path)
		}
		return strings.Join(paths, " ")
	}

	// Handle ${in[0]} case
	if strings.HasPrefix(ref, "in[") && strings.HasSuffix(ref, "]") {
		idxStr := ref[3 : len(ref)-1]
		if idx, err := strconv.Atoi(idxStr); err == nil {
			if input := task.getInputByIndex(idx); input != nil {
				return input.path
			}
		}
		return ""
	}

	// Handle ${in.foo} case
	if strings.HasPrefix(ref, "in.") {
		alias := ref[3:]
		if input := task.getInputByAlias(alias); input != nil {
			return input.path
		}
		return ""
	}

	return ""
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
			// TODO FIXME: tell user that updating the hash is meaningless if `out` is not supplied

			runFlowDefinitionProcessor(FLOW_DEFINITION_FILE, shouldUpdateOutSha256, shouldForceRun)
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

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
