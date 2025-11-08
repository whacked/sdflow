package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"embed"

	"github.com/spf13/cobra"

	"github.com/fatih/color"
	"github.com/google/go-jsonnet"
	"github.com/santhosh-tekuri/jsonschema/v5"
	yaml "gopkg.in/yaml.v3"
)

// Executor interface defines execution strategies
type Executor interface {
	// Core execution methods
	ExecuteCommand(task *RunnableTask, command string, env []string) error
	DownloadFile(url, outputPath string) error
	DownloadFileToStdout(url string) error
	ShouldUpdateSha256() bool
	ShouldForceRun() bool
	WorkerCount() int

	// Output methods for different execution phases
	ShowTaskStart(task *RunnableTask, taskLookup map[string]*RunnableTask)
	ShowTaskSkip(task *RunnableTask, reason string)
	ShowTaskCompleted(task *RunnableTask)

	// CAS integration
	GetCASStore() CASStore
	GetTaskMetadataStore() TaskMetadataStore
}

// RealExecutor performs actual execution
type RealExecutor struct {
	updateSha256      bool
	forceRun          bool
	workerCount       int
	casStore          CASStore
	taskMetadataStore TaskMetadataStore
}

func NewRealExecutor(updateSha256, forceRun bool) *RealExecutor {
	return NewRealExecutorWithWorkers(1, updateSha256, forceRun)
}

func NewRealExecutorWithWorkers(workerCount int, updateSha256, forceRun bool) *RealExecutor {
	casStore := NewFilesystemCASStore(CACHE_DIRECTORY)
	taskMetadataStore := NewFilesystemTaskMetadataStore(CACHE_DIRECTORY)
	return &RealExecutor{
		updateSha256:      updateSha256,
		forceRun:          forceRun,
		workerCount:       workerCount,
		casStore:          casStore,
		taskMetadataStore: taskMetadataStore,
	}
}

func (e *RealExecutor) ExecuteCommand(task *RunnableTask, command string, env []string) error {
	fmt.Fprint(
		os.Stderr,
		color.GreenString("Command: %s\n", command),
	)

	cmd := exec.Command("bash", "-c", command)
	cmd.Stderr = os.Stderr
	cmd.Env = env

	// Check if we should capture stdout to CAS (no out: field and either updateSha256 enabled or task is referenced)
	shouldCaptureToCAS := task.taskDeclaration.Out == nil && (e.updateSha256 || task.isReferenced)

	if shouldCaptureToCAS {
		// Capture stdout to CAS
		return e.executeWithCASCapture(task, cmd, command, env)
	} else {
		// Normal execution - stdout goes to terminal
		cmd.Stdout = os.Stdout
		err := cmd.Run()
		if err != nil {
			return fmt.Errorf("error executing command: %v", err)
		}
		return nil
	}
}

func (e *RealExecutor) executeWithCASCapture(task *RunnableTask, cmd *exec.Cmd, command string, env []string) error {
	// Compute action digest
	inputDigests := make(map[string]ContentDigest)
	for _, input := range task.inputs {
		if input.sha256 != "" {
			inputDigests[input.path] = ContentDigest(input.sha256)
		}
	}

	envMap := make(map[string]string)
	for _, envVar := range env {
		if parts := strings.SplitN(envVar, "=", 2); len(parts) == 2 {
			envMap[parts[0]] = parts[1]
		}
	}

	actionDigest := computeActionDigest(task.targetName, command, inputDigests, envMap)

	// Create temp file
	tempPath, err := e.casStore.CreateTempFile(actionDigest)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}

	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to open temp file: %v", err)
	}
	defer tempFile.Close()

	// Create hash writer
	hasher := sha256.New()

	// Use MultiWriter to write to both temp file and display stdout
	multiWriter := io.MultiWriter(tempFile, hasher, os.Stdout)
	cmd.Stdout = multiWriter

	// Execute command
	err = cmd.Run()
	exitCode := 0
	if err != nil {
		if exitError, ok := err.(*exec.ExitError); ok {
			exitCode = exitError.ExitCode()
		} else {
			os.Remove(tempPath)
			return fmt.Errorf("error executing command: %v", err)
		}
	}

	// Close temp file before finalizing
	tempFile.Close()

	if exitCode != 0 {
		// Command failed - remove temp file and return error
		os.Remove(tempPath)
		return fmt.Errorf("command exited with code %d", exitCode)
	}

	// Finalize temp file to CAS
	contentDigest, err := e.casStore.FinalizeTempFile(tempPath)
	if err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to finalize to CAS: %v", err)
	}

	// Store task metadata
	metadata := &TaskMetadata{
		ActionDigest:  actionDigest,
		InputDigests:  inputDigests,
		OutputDigests: map[string]ContentDigest{"stdout": contentDigest},
		EnvVars:       envMap,
		ExitCode:      exitCode,
		Timestamp:     time.Now(),
		ToolchainHash: TOOLCHAIN_FINGERPRINT,
	}

	if err := e.taskMetadataStore.Store(actionDigest, metadata); err != nil {
		return fmt.Errorf("failed to store task metadata: %v", err)
	}

	// Store the content digest in the task for later use
	task.casStdoutDigest = contentDigest

	return nil
}

func (e *RealExecutor) DownloadFile(url, outputPath string) error {
	return downloadFileToLocalPath(url, outputPath)
}

func (e *RealExecutor) DownloadFileToStdout(url string) error {
	return downloadFileToStdout(url)
}

func (e *RealExecutor) ShouldUpdateSha256() bool {
	return e.updateSha256
}

func (e *RealExecutor) ShouldForceRun() bool {
	return e.forceRun
}

func (e *RealExecutor) WorkerCount() int {
	return e.workerCount
}

func (e *RealExecutor) ShowTaskStart(task *RunnableTask, taskLookup map[string]*RunnableTask) {
	trace(fmt.Sprintf(
		"Running task: %+v (%d dependencies)\n", task.targetName, len(task.taskDependencies)))
	printVitalsForTask(task, taskLookup)
}

func (e *RealExecutor) ShowTaskSkip(task *RunnableTask, reason string) {
	fmt.Printf("Output for %s is up to date\n", task.targetName)
}

func (e *RealExecutor) ShowTaskCompleted(task *RunnableTask) {
}

func (e *RealExecutor) GetCASStore() CASStore {
	return e.casStore
}

func (e *RealExecutor) GetTaskMetadataStore() TaskMetadataStore {
	return e.taskMetadataStore
}

// DryRunExecutor simulates execution with pretty output
type DryRunExecutor struct {
	updateSha256      bool
	forceRun          bool
	workerCount       int
	casStore          CASStore
	taskMetadataStore TaskMetadataStore
}

func NewDryRunExecutor(updateSha256, forceRun bool) *DryRunExecutor {
	return NewDryRunExecutorWithWorkers(1, updateSha256, forceRun)
}

func NewDryRunExecutorWithWorkers(workerCount int, updateSha256, forceRun bool) *DryRunExecutor {
	casStore := NewFilesystemCASStore(CACHE_DIRECTORY)
	taskMetadataStore := NewFilesystemTaskMetadataStore(CACHE_DIRECTORY)
	return &DryRunExecutor{
		updateSha256:      updateSha256,
		forceRun:          forceRun,
		workerCount:       workerCount,
		casStore:          casStore,
		taskMetadataStore: taskMetadataStore,
	}
}

func tagDryRun(message string) string {
	return color.YellowString("[DRY RUN] %s", message)
}

func (e *DryRunExecutor) ExecuteCommand(task *RunnableTask, command string, env []string) error {
	fmt.Fprintf(os.Stderr, "%s %s\n", tagDryRun(task.targetName), command)
	return nil
}

func (e *DryRunExecutor) DownloadFile(url, outputPath string) error {
	fmt.Fprintf(os.Stderr, "%s %s -> %s\n", tagDryRun(url), url, outputPath)
	return nil
}

func (e *DryRunExecutor) DownloadFileToStdout(url string) error {
	fmt.Fprintf(os.Stderr, "%s %s -> stdout\n", tagDryRun(url), url)
	return nil
}

func (e *DryRunExecutor) ShouldUpdateSha256() bool {
	return e.updateSha256
}

func (e *DryRunExecutor) ShouldForceRun() bool {
	return e.forceRun
}

func (e *DryRunExecutor) WorkerCount() int {
	return e.workerCount
}

func (e *DryRunExecutor) ShowTaskStart(task *RunnableTask, taskLookup map[string]*RunnableTask) {
	fmt.Fprint(
		os.Stderr,
		tagDryRun(fmt.Sprintf("Running task: %+v (%d dependencies)\n", task.targetName, len(task.taskDependencies))))
	printVitalsForTask(task, taskLookup)
}

func (e *DryRunExecutor) ShowTaskSkip(task *RunnableTask, reason string) {
	fmt.Fprintf(os.Stderr, "%s SKIPPED %s\n", tagDryRun(task.targetName), reason)
}

func (e *DryRunExecutor) ShowTaskCompleted(task *RunnableTask) {
}

func (e *DryRunExecutor) GetCASStore() CASStore {
	return e.casStore
}

func (e *DryRunExecutor) GetTaskMetadataStore() TaskMetadataStore {
	return e.taskMetadataStore
}

// CAS (Content-Addressable Store) interfaces and types

// ActionDigest represents the hash of task execution parameters
type ActionDigest string

// ContentDigest represents the SHA-256 hash of task output content
type ContentDigest string

// TaskMetadata stores information about a completed task execution
type TaskMetadata struct {
	ActionDigest  ActionDigest             `json:"action_digest"`
	InputDigests  map[string]ContentDigest `json:"input_digests"`
	OutputDigests map[string]ContentDigest `json:"output_digests"`
	EnvVars       map[string]string        `json:"env_vars"`
	ExitCode      int                      `json:"exit_code"`
	StderrLogPath string                   `json:"stderr_log_path,omitempty"`
	Timestamp     time.Time                `json:"timestamp"`
	ToolchainHash string                   `json:"toolchain_hash"`
}

// CASStore interface for content-addressable storage operations
type CASStore interface {
	// Store content and return its digest
	Store(content []byte) (ContentDigest, error)

	// Retrieve content by digest
	Retrieve(digest ContentDigest) ([]byte, error)

	// Check if content exists
	Exists(digest ContentDigest) bool

	// Get the file path for a digest (for streaming)
	GetPath(digest ContentDigest) string

	// Create a temporary file for streaming content
	CreateTempFile(actionDigest ActionDigest) (string, error)

	// Finalize a temp file by computing its hash and moving to CAS
	FinalizeTempFile(tempPath string) (ContentDigest, error)
}

// FilesystemCASStore implements CASStore using local filesystem
type FilesystemCASStore struct {
	baseDir string
}

func NewFilesystemCASStore(baseDir string) *FilesystemCASStore {
	return &FilesystemCASStore{baseDir: baseDir}
}

// TaskMetadataStore interface for storing task execution metadata
type TaskMetadataStore interface {
	// Store metadata keyed by action digest
	Store(actionDigest ActionDigest, metadata *TaskMetadata) error

	// Retrieve metadata by action digest
	Retrieve(actionDigest ActionDigest) (*TaskMetadata, error)

	// Check if metadata exists
	Exists(actionDigest ActionDigest) bool
}

// FilesystemTaskMetadataStore implements TaskMetadataStore using local filesystem
type FilesystemTaskMetadataStore struct {
	baseDir string
}

func NewFilesystemTaskMetadataStore(baseDir string) *FilesystemTaskMetadataStore {
	return &FilesystemTaskMetadataStore{baseDir: baseDir}
}

// FilesystemCASStore implementation

func (c *FilesystemCASStore) Store(content []byte) (ContentDigest, error) {
	hash := sha256.Sum256(content)
	digest := ContentDigest(hex.EncodeToString(hash[:]))

	casPath := getCASObjectPath(c.baseDir, digest)
	if err := os.MkdirAll(filepath.Dir(casPath), 0755); err != nil {
		return "", err
	}

	// Write atomically using temp file + rename
	tempPath := casPath + ".part"
	if err := os.WriteFile(tempPath, content, 0644); err != nil {
		return "", err
	}

	if err := os.Chmod(tempPath, 0444); err != nil {
		os.Remove(tempPath)
		return "", err
	}

	if err := os.Rename(tempPath, casPath); err != nil {
		os.Remove(tempPath)
		return "", err
	}

	return digest, nil
}

func (c *FilesystemCASStore) Retrieve(digest ContentDigest) ([]byte, error) {
	casPath := getCASObjectPath(c.baseDir, digest)
	return os.ReadFile(casPath)
}

func (c *FilesystemCASStore) Exists(digest ContentDigest) bool {
	casPath := getCASObjectPath(c.baseDir, digest)
	_, err := os.Stat(casPath)
	return err == nil
}

func (c *FilesystemCASStore) GetPath(digest ContentDigest) string {
	return getCASObjectPath(c.baseDir, digest)
}

func (c *FilesystemCASStore) CreateTempFile(actionDigest ActionDigest) (string, error) {
	tempDir := filepath.Join(c.baseDir, "tmp")
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return "", err
	}

	timestamp := time.Now().Format("2006-01-02_150405.000")
	tempPath := filepath.Join(tempDir, fmt.Sprintf("%s.%s.part", actionDigest, timestamp))

	return tempPath, nil
}

func (c *FilesystemCASStore) FinalizeTempFile(tempPath string) (ContentDigest, error) {
	// Read the temp file and compute its hash
	content, err := os.ReadFile(tempPath)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(content)
	digest := ContentDigest(hex.EncodeToString(hash[:]))

	// Move to final CAS location
	casPath := getCASObjectPath(c.baseDir, digest)
	if err := os.MkdirAll(filepath.Dir(casPath), 0755); err != nil {
		return "", err
	}

	// Make file read-only and move atomically
	if err := os.Chmod(tempPath, 0444); err != nil {
		return "", err
	}

	if err := os.Rename(tempPath, casPath); err != nil {
		// If file already exists, just remove temp file
		if os.IsExist(err) {
			os.Remove(tempPath)
			return digest, nil
		}
		return "", err
	}

	return digest, nil
}

// FilesystemTaskMetadataStore implementation

func (t *FilesystemTaskMetadataStore) Store(actionDigest ActionDigest, metadata *TaskMetadata) error {
	metadataPath := getTaskMetadataPath(t.baseDir, actionDigest)
	if err := os.MkdirAll(filepath.Dir(metadataPath), 0755); err != nil {
		return err
	}

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(metadataPath, data, 0644)
}

func (t *FilesystemTaskMetadataStore) Retrieve(actionDigest ActionDigest) (*TaskMetadata, error) {
	metadataPath := getTaskMetadataPath(t.baseDir, actionDigest)
	data, err := os.ReadFile(metadataPath)
	if err != nil {
		return nil, err
	}

	var metadata TaskMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, err
	}

	return &metadata, nil
}

func (t *FilesystemTaskMetadataStore) Exists(actionDigest ActionDigest) bool {
	metadataPath := getTaskMetadataPath(t.baseDir, actionDigest)
	_, err := os.Stat(metadataPath)
	return err == nil
}

// Toolchain constants
const TOOLCHAIN_FINGERPRINT = "sdflow-0.0.1-placeholder"

// Git-style path partitioning utilities

// partitionDigest splits a digest into git-style path components: ab/cd/rest
func partitionDigest(digest string) (string, string, string) {
	if len(digest) < 4 {
		// Handle short digests by padding or using as-is
		return digest, "", ""
	}
	return digest[:2], digest[2:4], digest[4:]
}

// digestToGitPath converts a digest to git-style partitioned path
func digestToGitPath(digest string) string {
	prefix1, prefix2, rest := partitionDigest(digest)
	if rest == "" {
		if prefix2 == "" {
			return prefix1
		}
		return filepath.Join(prefix1, prefix2)
	}
	return filepath.Join(prefix1, prefix2, rest)
}

// computeActionDigest computes the action digest for a task
func computeActionDigest(taskName, expandedRun string, inputDigests map[string]ContentDigest, envVars map[string]string) ActionDigest {
	hasher := sha256.New()

	// Task name
	hasher.Write([]byte(taskName))
	hasher.Write([]byte{0}) // separator

	// Expanded run command
	hasher.Write([]byte(expandedRun))
	hasher.Write([]byte{0})

	// Input digests (sorted for determinism)
	var inputKeys []string
	for k := range inputDigests {
		inputKeys = append(inputKeys, k)
	}
	sort.Strings(inputKeys)

	for _, key := range inputKeys {
		hasher.Write([]byte(key))
		hasher.Write([]byte{0})
		hasher.Write([]byte(inputDigests[key]))
		hasher.Write([]byte{0})
	}

	// Environment variables (sorted for determinism)
	var envKeys []string
	for k := range envVars {
		envKeys = append(envKeys, k)
	}
	sort.Strings(envKeys)

	for _, key := range envKeys {
		hasher.Write([]byte(key))
		hasher.Write([]byte{0})
		hasher.Write([]byte(envVars[key]))
		hasher.Write([]byte{0})
	}

	// Toolchain fingerprint
	hasher.Write([]byte(TOOLCHAIN_FINGERPRINT))

	return ActionDigest(hex.EncodeToString(hasher.Sum(nil)))
}

// getCASObjectPath returns the CAS path for a content digest
func getCASObjectPath(baseDir string, digest ContentDigest) string {
	partitionedPath := digestToGitPath(string(digest))
	return filepath.Join(baseDir, "objects", "sha256", partitionedPath)
}

// getTaskMetadataPath returns the metadata path for an action digest
func getTaskMetadataPath(baseDir string, digest ActionDigest) string {
	partitionedPath := digestToGitPath(string(digest))
	return filepath.Join(baseDir, "tasks", partitionedPath+".json")
}

// getTaskStdoutDigestFromCAS checks if a task's stdout was captured to CAS and returns the content digest
func getTaskStdoutDigestFromCAS(task *RunnableTask, executor Executor) (ContentDigest, bool) {
	if task.casStdoutDigest != "" {
		return task.casStdoutDigest, true
	}
	return "", false
}

// isTaskReference checks if an input string refers to a task name for CAS caching
// (only tasks without explicit out: fields that capture stdout to CAS)
func isTaskReference(input string, taskLookup map[string]*RunnableTask) bool {
	// Task references should:
	// 1. Exist in the task lookup
	// 2. Not be a file path (no path separators or extensions)
	// 3. The referenced task should have no explicit 'out:' field (stdout capture only)

	if strings.Contains(input, "/") || strings.Contains(input, ".") {
		// Contains path separators or file extensions, likely a file path
		return false
	}

	if referencedTask, exists := taskLookup[input]; exists {
		// Task exists, check if it has no explicit out: field (stdout capture only)
		return referencedTask.taskDeclaration != nil && referencedTask.taskDeclaration.Out == nil
	}

	return false
}

// isTaskDependency checks if an input string refers to a task name for dependency resolution
// (any task name that can be referenced as a dependency)
func isTaskDependency(input string, taskLookup map[string]*RunnableTask) bool {
	// Task dependencies should:
	// 1. Exist in the task lookup
	// 2. Not be a file path (no path separators or extensions)
	// 3. Be a real task name (not an output file alias)

	if strings.Contains(input, "/") || strings.Contains(input, ".") {
		// Contains path separators or file extensions, likely a file path
		return false
	}

	if referencedTask, exists := taskLookup[input]; exists {
		// Task exists, check if it's a real task (has taskDeclaration) and not just an output file alias
		return referencedTask.taskDeclaration != nil && referencedTask.targetName == input
	}

	return false
}

// declare list of candidates for the flow definition file
var FLOW_DEFINITION_FILE_CANDIDATES = []string{
	"Sdflow.yaml",
	"sdflow.yaml",
	"Sdflow.yml",
	"sdflow.yml",
	"Sdflow.jsonnet",
	"sdflow.jsonnet",
	"Sdflow.json",
	"sdflow.json",
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

// maxRelativePathDepth defines the maximum number of "../" segments allowed
// before a path is converted to absolute or home-relative form for display
const maxRelativePathDepth = 3

type TaskExecutionState int

const (
	TaskNotStarted TaskExecutionState = iota
	TaskInProgress                    // currently executing (prevents re-entry in cycles)
	TaskCompleted
	TaskSkipped // up-to-date
)

type RunnableTaskInput struct {
	path          string
	sha256        string
	mtime         int64
	alias         string // New field for named inputs
	taskReference string // If this input is a task reference, store the task name
	originalInput string // The original input value before path processing
}

type RunnableTask struct {
	taskDeclaration  *RunnableSchemaJson
	taskDependencies []*RunnableTask
	targetKey        string // the original key
	targetName       string
	outTime          int64
	inputs           []*RunnableTaskInput
	executionState   TaskExecutionState
	executionCount   int           // for testing
	casStdoutDigest  ContentDigest // CAS digest of captured stdout (for tasks without out:)
	isReferenced     bool          // true if this task is referenced by other tasks
}

// Helper functions for handling multiple input/output types (shared logic)

// extractPathsFromField extracts all path strings from a field that can be:
// - string: returns single-element slice
// - []interface{}: extracts all strings from array
// - map[string]interface{}: extracts all string values (sorted by key for consistency)
// This is used for both inputs and outputs to avoid duplication
func extractPathsFromField(field interface{}) []string {
	if field == nil {
		return nil
	}

	switch v := field.(type) {
	case string:
		return []string{v}
	case []interface{}:
		paths := make([]string, 0, len(v))
		for _, item := range v {
			if str, ok := item.(string); ok {
				paths = append(paths, str)
			}
		}
		return paths
	case map[string]interface{}:
		// For maps, return all values in a consistent order (sorted by key)
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		paths := make([]string, 0, len(keys))
		for _, k := range keys {
			if str, ok := v[k].(string); ok {
				paths = append(paths, str)
			}
		}
		return paths
	default:
		return nil
	}
}

// processPathString processes a single path string with variable substitution and normalization
// This is shared logic for both inputs and outputs
func processPathString(pathStr string, env map[string]string) string {
	if isPath(pathStr) {
		// If it starts with tilde, expand it
		if strings.HasPrefix(pathStr, "~") {
			return getPathRelativeToCwd(pathStr)
		}
		// For other paths (absolute, ./, ../), return as-is
		return pathStr
	}
	return getPathRelativeToCwd(*substituteWithContext(pathStr, env))
}

// processFieldForPaths processes a field (string/array/map) and normalizes all paths
// Returns the field in the same structure but with processed paths
func processFieldForPaths(field interface{}, env map[string]string) interface{} {
	if field == nil {
		return nil
	}

	switch v := field.(type) {
	case string:
		return processPathString(v, env)
	case []interface{}:
		// Process each element in the array
		processed := make([]interface{}, len(v))
		for i, item := range v {
			if str, ok := item.(string); ok {
				processed[i] = processPathString(str, env)
			} else {
				processed[i] = item
			}
		}
		return processed
	case map[string]interface{}:
		// Process each value in the map
		processed := make(map[string]interface{})
		for key, val := range v {
			if str, ok := val.(string); ok {
				processed[key] = processPathString(str, env)
			} else {
				processed[key] = val
			}
		}
		return processed
	default:
		return field
	}
}

// getOutputPaths returns all output file paths from the Out field
// Returns nil if Out is nil, otherwise returns a slice of paths
func getOutputPaths(out interface{}) []string {
	return extractPathsFromField(out)
}

// getPrimaryOutputPath returns the first/primary output path for display purposes
func getPrimaryOutputPath(out interface{}) string {
	paths := getOutputPaths(out)
	if len(paths) > 0 {
		return paths[0]
	}
	return ""
}

// getOutputSha256Map returns a map of output file paths to their SHA256 hashes
func getOutputSha256Map(out interface{}, outSha256 interface{}) map[string]string {
	if outSha256 == nil {
		return nil
	}

	result := make(map[string]string)

	switch sha := outSha256.(type) {
	case string:
		// Single SHA256 for single output
		if outStr, ok := out.(string); ok {
			result[outStr] = sha
		}
	case map[string]interface{}:
		// Map of SHA256s
		for key, val := range sha {
			if shaStr, ok := val.(string); ok {
				// If out is a map, use the mapped path
				if outMap, ok := out.(map[string]interface{}); ok {
					if pathStr, ok := outMap[key].(string); ok {
						result[pathStr] = shaStr
					}
				}
			}
		}
	}

	return result
}

// hasOutput returns true if the task has any output (file or CAS)
func hasOutput(taskDecl *RunnableSchemaJson) bool {
	return taskDecl.Out != nil || taskDecl.OutSha256 != nil
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
		fmt.Println("  Out:", task.taskDeclaration.Out)
	}
	if task.taskDeclaration.Run != nil {
		fmt.Println("  Run:", *task.taskDeclaration.Run)
	}
	if task.taskDeclaration.InSha256 != nil {
		fmt.Println("  In SHA256:", task.taskDeclaration.InSha256)
	}
	if task.taskDeclaration.OutSha256 != nil {
		fmt.Println("  Out SHA256:", task.taskDeclaration.OutSha256)
	}
}

func isSimpleDownloadTask(task *RunnableTask) bool {
	if task.taskDeclaration == nil {
		return false
	}

	// Must have no run command
	if task.taskDeclaration.Run != nil {
		return false
	}

	// Must have at least one remote input
	hasRemoteInput := false
	for _, taskInput := range task.inputs {
		if isRemotePath(taskInput.path) {
			hasRemoteInput = true
			break
		}
	}

	return hasRemoteInput
}

func checkIfOutputMoreRecentThanInputs(task *RunnableTask) bool {
	if task.taskDeclaration == nil {
		return false
	}

	// Handle tasks with explicit output files
	if task.taskDeclaration.Out != nil {
		outputPaths := getOutputPaths(task.taskDeclaration.Out)
		if len(outputPaths) == 0 {
			return false
		}

		// For multiple outputs: ALL outputs must be newer than ALL inputs
		// Find the minimum (oldest) output mtime across all outputs
		var minOutputTime int64 = -1
		for _, outputPath := range outputPaths {
			outputStat, err := os.Stat(outputPath)
			if err != nil {
				// If any output file doesn't exist, task needs to run
				return false
			}
			outputTime := outputStat.ModTime().Unix()
			if minOutputTime == -1 || outputTime < minOutputTime {
				minOutputTime = outputTime
			}
		}

		// Check all inputs - if any input is newer than the oldest output, task needs to run
		for _, taskInput := range task.inputs {
			if inputStat, err := os.Stat(taskInput.path); err == nil {
				taskInput.mtime = inputStat.ModTime().Unix()
				if minOutputTime < taskInput.mtime {
					return false // Input is newer than (at least one) output
				}
			}
		}
		return true
	}

	// Handle CAS-cached tasks (no explicit out: but has out.sha256)
	if task.taskDeclaration.Out == nil && task.taskDeclaration.OutSha256 != nil {
		// For CAS tasks with single SHA256
		if shaStr, ok := task.taskDeclaration.OutSha256.(string); ok {
			casPath := getCASObjectPath(CACHE_DIRECTORY, ContentDigest(shaStr))
			if casStat, err := os.Stat(casPath); err == nil {
				casTime := casStat.ModTime().Unix()

				// Check all inputs - if any input is newer than CAS object, task needs to run
				for _, taskInput := range task.inputs {
					if inputStat, err := os.Stat(taskInput.path); err == nil {
						taskInput.mtime = inputStat.ModTime().Unix()
						if casTime <= taskInput.mtime {
							return false // Input is newer than CAS cache
						}
					}
				}
				return true // CAS cache is up to date
			}
			return false // CAS object doesn't exist
		}

		// For CAS tasks with map of SHA256s
		if shaMap, ok := task.taskDeclaration.OutSha256.(map[string]interface{}); ok {
			// Find minimum CAS time across all CAS objects
			var minCasTime int64 = -1
			for _, shaVal := range shaMap {
				if shaStr, ok := shaVal.(string); ok {
					casPath := getCASObjectPath(CACHE_DIRECTORY, ContentDigest(shaStr))
					if casStat, err := os.Stat(casPath); err == nil {
						casTime := casStat.ModTime().Unix()
						if minCasTime == -1 || casTime < minCasTime {
							minCasTime = casTime
						}
					} else {
						// If any CAS object doesn't exist, task needs to run
						return false
					}
				}
			}

			if minCasTime == -1 {
				return false
			}

			// Check all inputs against minimum CAS time
			for _, taskInput := range task.inputs {
				if inputStat, err := os.Stat(taskInput.path); err == nil {
					taskInput.mtime = inputStat.ModTime().Unix()
					if minCasTime <= taskInput.mtime {
						return false
					}
				}
			}
			return true
		}
	}

	// Task has no output (neither explicit file nor CAS cache)
	return false
}

// noColorString returns the formatted string without any color codes
func noColorString(format string, a ...interface{}) string {
	return fmt.Sprintf(format, a...)
}

// checkIfInputCausesStaleness determines if a specific input file is newer than the task's output
func checkIfInputCausesStaleness(task *RunnableTask, taskInput *RunnableTaskInput) bool {
	if task.taskDeclaration == nil {
		return false
	}

	// Handle tasks with explicit output files
	if task.taskDeclaration.Out != nil {
		outputPaths := getOutputPaths(task.taskDeclaration.Out)
		if len(outputPaths) == 0 {
			return true
		}

		// For multiple outputs: check if input is newer than ANY output
		// Find the minimum (oldest) output mtime
		var minOutputTime int64 = -1
		for _, outputPath := range outputPaths {
			outputStat, err := os.Stat(outputPath)
			if err != nil {
				// If any output file doesn't exist, input causes staleness
				return true
			}
			outputTime := outputStat.ModTime().Unix()
			if minOutputTime == -1 || outputTime < minOutputTime {
				minOutputTime = outputTime
			}
		}

		// Check if this specific input is newer than the oldest output
		if inputStat, err := os.Stat(taskInput.path); err == nil {
			inputTime := inputStat.ModTime().Unix()
			return minOutputTime < inputTime
		}
	}

	// For CAS-cached tasks, we can't easily determine individual input staleness
	// since the output is based on content hash, not timestamps
	return false
}

func getInputColor(taskInput *RunnableTaskInput, taskLookup map[string]*RunnableTask) func(format string, a ...interface{}) string {
	// Non-task references use default terminal color (no color codes)
	if taskInput.taskReference == "" {
		return noColorString
	}

	// Look up the referenced task
	referencedTask, exists := taskLookup[taskInput.taskReference]
	if !exists {
		return noColorString // Fallback to default if task not found
	}

	// Check if referenced task is up-to-date
	if checkIfOutputMoreRecentThanInputs(referencedTask) {
		return color.GreenString // Current/up-to-date
	} else {
		return color.RedString // Stale/needs update
	}
}

func printVitalsForTask(task *RunnableTask, taskLookup map[string]*RunnableTask) {
	if task.taskDeclaration == nil {
		return
	}

	var upToDateString string
	var coloringFunc func(format string, a ...interface{}) string
	if checkIfOutputMoreRecentThanInputs(task) {
		// Check if this is a CAS-cached task (has out.sha256 but no explicit out:)
		if task.taskDeclaration.Out == nil && task.taskDeclaration.OutSha256 != nil {
			upToDateString = color.HiCyanString("cached")
		} else {
			upToDateString = color.GreenString("current")
		}
		coloringFunc = color.HiGreenString
	} else if task.taskDeclaration.Out == nil {
		upToDateString = ""
		coloringFunc = color.CyanString
	} else {
		upToDateString = color.RedString("stale")
		coloringFunc = color.HiRedString
	}

	fmt.Fprintf(os.Stdout,
		"╭─❮❮ %s ❯❯\n",
		coloringFunc("%s", task.targetName),
	)

	// Determine if task is stale (for staleness markers)
	isTaskStale := task.taskDeclaration.Out != nil && !checkIfOutputMoreRecentThanInputs(task)

	for _, taskInput := range task.inputs {
		inputPath := normalizePathForDisplay(taskInput.path)

		// Check if this input causes staleness and override color
		if isTaskStale && checkIfInputCausesStaleness(task, taskInput) {
			fmt.Fprintf(os.Stdout,
				"├ %s\n",
				color.HiMagentaString("%s", inputPath),
			)
		} else {
			colorFunc := getInputColor(taskInput, taskLookup)
			fmt.Fprintf(os.Stdout,
				"├ %s\n",
				colorFunc("%s", inputPath),
			)
		}
	}

	// Add run command if present, or show NO RUN COMMAND warning (except for simple download tasks)
	if task.taskDeclaration.Run != nil {
		runCommand := *task.taskDeclaration.Run
		// Show only first line if multi-line, with ellipsis
		if strings.Contains(runCommand, "\n") {
			lines := strings.Split(runCommand, "\n")
			runCommand = lines[0] + "..."
		}
		fmt.Fprintf(os.Stdout,
			"├─◁ %s\n",
			color.YellowString("%s", runCommand),
		)
	} else if !isSimpleDownloadTask(task) {
		fmt.Fprintf(os.Stdout,
			"│ %s\n",
			color.New(color.FgHiYellow, color.Bold).Sprint("!! NO RUN COMMAND !!"),
		)
	}

	fmt.Fprintf(os.Stdout, "│\n")
	fmt.Fprintf(os.Stdout, "╰─▶ ")
	if task.taskDeclaration.Out == nil {
		fmt.Fprint(
			os.Stdout,
			color.CyanString("%s", "<STDOUT>"),
		)
	} else {
		// For multiple outputs, show all of them
		outputPaths := getOutputPaths(task.taskDeclaration.Out)
		if len(outputPaths) == 1 {
			fmt.Fprintf(os.Stdout,
				"%s",
				color.HiBlueString("%s", normalizePathForDisplay(outputPaths[0])),
			)
		} else if len(outputPaths) > 1 {
			fmt.Fprintf(os.Stdout, "[")
			for i, outPath := range outputPaths {
				if i > 0 {
					fmt.Fprintf(os.Stdout, ", ")
				}
				fmt.Fprintf(os.Stdout,
					"%s",
					color.HiBlueString("%s", normalizePathForDisplay(outPath)),
				)
			}
			fmt.Fprintf(os.Stdout, "]")
		}
	}
	if upToDateString != "" {
		fmt.Fprintf(os.Stdout, " (%s)", upToDateString)
	}

	fmt.Fprintf(os.Stdout, "\n\n")
}

func substituteWithContext(s string, context map[string]string) *string {
	// Handle $$ escape sequences by temporarily replacing them
	const placeholder = "___ESCAPED_DOLLAR___"
	escapedTemplate := strings.ReplaceAll(s, "$$", placeholder)

	mapper := func(varName string) string {
		return context[varName]
	}

	expanded := os.Expand(escapedTemplate, mapper)
	// Restore escaped dollars
	substituted := strings.ReplaceAll(expanded, placeholder, "$")
	return &substituted
}

// renderCommandWithEnv renders a task's run command with controlled builtin substitution
// and returns both the rendered command and environment variables for injection.
// This separates sdflow builtins (substituted in command) from YAML globals (passed as env vars).
func renderCommandWithEnv(task *RunnableTask, env map[string][]string) (string, []string, error) {
	// Separate builtin task variables from YAML globals
	taskBuiltins := make(map[string][]string)
	var envVars []string

	// Add task-specific builtin variables
	if task.taskDeclaration.Out != nil {
		outputPaths := getOutputPaths(task.taskDeclaration.Out)
		taskBuiltins["out"] = outputPaths

		// For map outputs, also add named aliases like "out.first", "out.second"
		if outMap, ok := task.taskDeclaration.Out.(map[string]interface{}); ok {
			for key, val := range outMap {
				if pathStr, ok := val.(string); ok {
					aliasKey := fmt.Sprintf("out.%s", key)
					taskBuiltins[aliasKey] = []string{pathStr}
				}
			}
		}
	}

	// Add input variables
	var inPaths []string
	for _, input := range task.inputs {
		resolvedPath := input.path

		// If this is a task reference, resolve it to the CAS path
		if input.taskReference != "" {
			// Find the referenced task and get its CAS digest
			for _, dep := range task.taskDependencies {
				if dep.targetName == input.taskReference {
					var contentDigest ContentDigest

					// First try to get from runtime CAS digest (for real execution)
					if dep.casStdoutDigest != "" {
						contentDigest = dep.casStdoutDigest
					} else if dep.taskDeclaration != nil && dep.taskDeclaration.OutSha256 != nil {
						// For dry-run or when task isn't executed yet, use out.sha256 from YAML
						// Handle both string and map SHA256
						if shaStr, ok := dep.taskDeclaration.OutSha256.(string); ok {
							contentDigest = ContentDigest(shaStr)
						}
					}

					if contentDigest != "" {
						// Get the CAS path for this content digest
						casPath := getCASObjectPath(CACHE_DIRECTORY, contentDigest)
						resolvedPath = casPath
						break
					}
				}
			}
		}

		inPaths = append(inPaths, resolvedPath)

		// Add input alias variables like "in.first", "in.second"
		if input.alias != "" {
			aliasKey := fmt.Sprintf("in.%s", input.alias)
			taskBuiltins[aliasKey] = []string{resolvedPath}
		}

		// NOTE: Removed in_0, in_1 syntax - use ${in.0}, ${in.1} instead
	}
	taskBuiltins["in"] = inPaths

	// Convert YAML globals to environment variables
	// Start with system environment
	envVars = append(envVars, os.Environ()...)

	// Add YAML globals, which override system environment
	for key, values := range env {
		// Skip core builtin task variables - they shouldn't be in environment
		// Note: in_N variables are no longer built-in, so they can appear in environment
		if key == "in" || key == "out" || (strings.HasPrefix(key, "in.") && isValidAlias(key[3:])) {
			continue
		}
		// Convert array values to space-separated string for environment
		envValue := strings.Join(values, " ")
		envVars = append(envVars, fmt.Sprintf("%s=%s", key, envValue))
	}

	if task.taskDeclaration.Run != nil {
		trace(fmt.Sprintf("Run command: %s", *task.taskDeclaration.Run))
	}

	// Use controlled builtin substitution instead of aggressive expansion
	renderedCommand, err := expandSdflowBuiltins(*task.taskDeclaration.Run, taskBuiltins)
	if err != nil {
		return "", nil, fmt.Errorf("error expanding builtin variables: %v", err)
	}
	return renderedCommand, envVars, nil
}

// isValidAlias checks if a string is a valid input alias (not a numeric index)
func isValidAlias(alias string) bool {
	if alias == "" {
		return false
	}
	// If it's a number, it's not an alias (it would be handled by ${in.N} syntax)
	if _, err := strconv.Atoi(alias); err == nil {
		return false
	}
	// Check if it matches alias naming rules
	match, _ := regexp.MatchString(`^[a-zA-Z_][a-zA-Z0-9_]*$`, alias)
	return match
}

// renderCommand provides backward compatibility by using the new controlled substitution
// but maintaining the original function signature for existing code.
func renderCommand(task *RunnableTask, env map[string][]string) string {
	command, _, err := renderCommandWithEnv(task, env)
	if err != nil {
		// For backward compatibility, log the error and return the original command
		trace(fmt.Sprintf("Error rendering command: %v", err))
		if task.taskDeclaration.Run != nil {
			return *task.taskDeclaration.Run
		}
		return ""
	}
	return command
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
		return getCASObjectPath(CACHE_DIRECTORY, ContentDigest(sha256Value)), true

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

		return getCASObjectPath(CACHE_DIRECTORY, ContentDigest(sha256)), true

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

func runTask(task *RunnableTask, env map[string][]string, executor Executor, taskLookup map[string]*RunnableTask) {

	// Check execution state first - prevent duplicate execution and cyclic re-entry
	if task.executionState == TaskCompleted || task.executionState == TaskSkipped {
		return
	}

	// If already in progress, we've hit a cycle - stop to prevent infinite loop
	if task.executionState == TaskInProgress {
		return
	}

	// Check if up-to-date (existing logic)
	if task.executionState == TaskNotStarted {
		if !executor.ShouldForceRun() && checkIfOutputMoreRecentThanInputs(task) {
			task.executionState = TaskSkipped
			executor.ShowTaskSkip(task, "up-to-date")
			return
		}
	}

	// Mark as in progress to prevent re-entry during cyclic dependency resolution
	task.executionState = TaskInProgress

	executor.ShowTaskStart(task, taskLookup)

	for _, dep := range task.taskDependencies {
		trace(fmt.Sprintf("Running dependency: %s", dep.targetName))
		runTask(dep, env, executor, taskLookup)
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
		} else if task.taskDeclaration.Run == nil && isRemotePath(task.inputs[0].path) {
			trace("running download task")

			if task.taskDeclaration.Out != nil {
				// File download - handle single output only for now
				primaryOutput := getPrimaryOutputPath(task.taskDeclaration.Out)
				if primaryOutput != "" {
					shouldDownloadFile := false
					if _, err := os.Stat(primaryOutput); err == nil {
						// Check SHA256 if specified
						sha256Map := getOutputSha256Map(task.taskDeclaration.Out, task.taskDeclaration.OutSha256)
						if expectedSha, ok := sha256Map[primaryOutput]; ok {
							if !isFileBytesMatchingSha256(primaryOutput, expectedSha) {
								fmt.Fprintf(os.Stderr, "warning: SHA256 mismatch for:\n%s; overwriting file", primaryOutput)
								shouldDownloadFile = true
							}
						}
					} else {
						shouldDownloadFile = true
					}

					if shouldDownloadFile {
						err := executor.DownloadFile(task.inputs[0].path, primaryOutput)
						if err != nil {
							fmt.Printf("Error downloading file: %v\n", err)
							return
						}
						shouldCheckOutput = true
					}
				}
			} else {
				// Stdout download
				err := executor.DownloadFileToStdout(task.inputs[0].path)
				if err != nil {
					fmt.Printf("Error downloading file to stdout: %v\n", err)
					return
				}
			}
		}
	}

	if task.taskDeclaration.Run != nil {
		command, cmdEnv, err := renderCommandWithEnv(task, env)
		if err != nil {
			fmt.Printf("Error rendering command for task %s: %v\n", task.targetName, err)
			return
		}

		err = executor.ExecuteCommand(task, command, cmdEnv)
		if err != nil {
			fmt.Printf("Error executing command: %v\n", err)
			return
		}
		shouldCheckOutput = true
	}

	if executor.ShouldUpdateSha256() && task.taskDeclaration.Out != nil {
		// TODO: Add full support for updating SHA256 of multiple outputs
		// For now, handle single output only
		primaryOutput := getPrimaryOutputPath(task.taskDeclaration.Out)
		if primaryOutput != "" {
			outputFileBytes, err := os.ReadFile(primaryOutput)
			bailOnError(err)
			outputSha256 := getBytesSha256(outputFileBytes)
			// For single string output, update as string
			if _, ok := task.taskDeclaration.Out.(string); ok {
				task.taskDeclaration.OutSha256 = outputSha256
			}
			trace(fmt.Sprintf("Updated OUT SHA256: %s", outputSha256))
		}
	} else if shouldCheckOutput && task.taskDeclaration.Out != nil && task.taskDeclaration.OutSha256 != nil {
		// Check SHA256 for all outputs
		sha256Map := getOutputSha256Map(task.taskDeclaration.Out, task.taskDeclaration.OutSha256)
		for outputPath, expectedSha := range sha256Map {
			if isFileBytesMatchingSha256(outputPath, expectedSha) {
				trace(fmt.Sprintf("OUT SHA256 matches for %s!", outputPath))
			} else {
				trace(fmt.Sprintf("OUT SHA256 mismatch for %s!", outputPath))
			}
		}
	}

	// Handle SHA256 updates for referenced tasks (automatic caching) or when --updatehash is used
	if executor.ShouldUpdateSha256() || task.isReferenced {
		var needsUpdate bool
		var sha256ToUpdate string

		if task.taskDeclaration.OutSha256 != nil {
			// Traditional case: task has explicit out: field
			// For single output, extract the SHA256 string
			if shaStr, ok := task.taskDeclaration.OutSha256.(string); ok {
				needsUpdate = true
				sha256ToUpdate = shaStr
			}
		} else if task.taskDeclaration.Out == nil {
			// New CAS case: task has no out: field, check if stdout was captured
			if contentDigest, ok := getTaskStdoutDigestFromCAS(task, executor); ok {
				needsUpdate = true
				sha256ToUpdate = string(contentDigest)
				// Set the OutSha256 in the task declaration so updateOutSha256ForTarget can find it
				task.taskDeclaration.OutSha256 = sha256ToUpdate
			}
		}

		if needsUpdate {
			updatedYamlString := updateOutSha256ForTarget(FLOW_DEFINITION_FILE, task.targetKey, sha256ToUpdate)
			outputYamlString := addInterveningSpacesToRootLevelBlocks(updatedYamlString)
			// re-output the file
			currentFileMode := os.ModePerm
			if fileInfo, err := os.Stat(FLOW_DEFINITION_FILE); err == nil {
				currentFileMode = fileInfo.Mode()
			}
			err := os.WriteFile(FLOW_DEFINITION_FILE, []byte(outputYamlString), currentFileMode)
			bailOnError(err)
			trace(fmt.Sprintf("Updated YAML file with new SHA256: %s", sha256ToUpdate))
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

	// Expand tilde before processing
	expandedPath := expandTilde(path)

	cwd, err := os.Getwd()
	bailOnError(err)
	absPath, err := filepath.Abs(expandedPath)
	bailOnError(err)
	relPath, err := filepath.Rel(cwd, absPath)
	bailOnError(err)
	if !strings.HasPrefix(relPath, ".") {
		relPath = "./" + relPath
	}
	return relPath
}

// normalizePathForDisplay ensures consistent "./" prefix for relative local paths
// and converts paths that were likely absolute back to absolute form for display.
// For deeply nested relative paths (>= maxRelativePathDepth "../" segments),
// it converts them to absolute paths, or if under $HOME, to $HOME-relative form.
func normalizePathForDisplay(path string) string {
	// Don't modify URLs
	if isRemotePath(path) {
		return path
	}

	// Keep absolute paths as-is
	if strings.HasPrefix(path, "/") {
		return path
	}

	// If path has many "../" segments, it was likely an absolute path converted to relative
	// Convert it to a more readable form
	if strings.Count(path, "../") >= maxRelativePathDepth {
		absPath, err := filepath.Abs(path)
		if err != nil {
			// If we can't get absolute path, just return as-is
			return path
		}

		// Check if the absolute path is under $HOME
		home, err := os.UserHomeDir()
		if err == nil && strings.HasPrefix(absPath, home+string(filepath.Separator)) {
			// Path is under home directory - use $HOME-relative form
			// Using $HOME instead of ~ for cross-platform compatibility
			relToHome := strings.TrimPrefix(absPath, home)
			return "$HOME" + relToHome
		}

		// Path is not under home - use absolute path
		return absPath
	}

	// Ensure relative paths start with "./" for consistency
	if strings.HasPrefix(path, "./") || strings.HasPrefix(path, "../") {
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
		outputPaths := getOutputPaths(task.taskDeclaration.Out)
		// For multiple outputs, use the minimum (oldest) mtime
		var minOutputTime int64 = -1
		for _, outputPath := range outputPaths {
			stat, err := os.Stat(outputPath)
			if err == nil {
				outputTime := stat.ModTime().Unix()
				if minOutputTime == -1 || outputTime < minOutputTime {
					minOutputTime = outputTime
				}
			}
		}
		if minOutputTime != -1 {
			task.outTime = minOutputTime
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
		task.taskDeclaration.Out = fileAbsPath
	} else {
		if outputPathValue, ok := runnableData["out"]; ok {
			// Use shared helper to process paths in the field (string/array/map)
			task.taskDeclaration.Out = processFieldForPaths(outputPathValue, convertArrayMapToStringMap(executionEnv))
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
						path:          inString,
						alias:         inString,
						originalInput: expandedInput,
					})
				}
			}
			concatenatedInputs := strings.Join(inputStrings, " ")
			task.taskDeclaration.In = &concatenatedInputs
		case map[string]interface{}:
			// map of alias -> input path
			var inputStrings []string
			for alias, inItem := range v {
				originalInput := inItem.(string)
				// Use shared helper for path processing
				inString := processPathString(originalInput, convertArrayMapToStringMap(executionEnv))
				inputStrings = append(inputStrings, fmt.Sprintf("\"%s\"", inString))
				task.inputs = append(task.inputs, &RunnableTaskInput{
					path:          inString,
					alias:         alias,
					originalInput: originalInput,
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
					path:          inString,
					originalInput: expandedInput,
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
		// Handle different SHA256 types: string or map
		task.taskDeclaration.OutSha256 = outSha256Value
	}

	if runnableValue, ok := runnableData["run"]; ok {
		runString := runnableValue.(string)
		task.taskDeclaration.Run = &runString
	}
	populateTaskModTimes(&task)

	return &task
}

// detectFileType determines the file type based on extension
func detectFileType(filePath string) string {
	if strings.HasSuffix(filePath, ".jsonnet") {
		return "jsonnet"
	} else if strings.HasSuffix(filePath, ".json") {
		return "json"
	} else if strings.HasSuffix(filePath, ".yaml") || strings.HasSuffix(filePath, ".yml") {
		return "yaml"
	}
	return "yaml" // default fallback
}

// renderJsonnetToJson processes a jsonnet file and returns JSON string
func renderJsonnetToJson(filePath string) (string, error) {
	vm := jsonnet.MakeVM()

	// Set import callback for relative imports
	vm.Importer(&jsonnet.FileImporter{
		JPaths: []string{filepath.Dir(filePath)}, // Allow imports from same directory
	})

	jsonStr, err := vm.EvaluateFile(filePath)
	if err != nil {
		return "", err
	}

	return jsonStr, nil
}

func parseFlowDefinitionFile(flowDefinitionFilePath string) *ParsedFlowDefinition {
	fileType := detectFileType(flowDefinitionFilePath)

	var flowDefinitionSource string

	switch fileType {
	case "jsonnet":
		// Render jsonnet to JSON first
		jsonStr, err := renderJsonnetToJson(flowDefinitionFilePath)
		bailOnError(err)
		flowDefinitionSource = jsonStr

	case "json":
		// Read JSON file directly
		fileBytes, err := os.ReadFile(flowDefinitionFilePath)
		bailOnError(err)
		flowDefinitionSource = string(fileBytes)

	case "yaml":
		// Read YAML file directly
		fileBytes, err := os.ReadFile(flowDefinitionFilePath)
		bailOnError(err)
		flowDefinitionSource = string(fileBytes)

	default:
		// Fallback to reading as YAML
		fileBytes, err := os.ReadFile(flowDefinitionFilePath)
		bailOnError(err)
		flowDefinitionSource = string(fileBytes)
	}

	return parseFlowDefinitionSource(flowDefinitionSource)
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
				// Index task by all output paths
				outputPaths := getOutputPaths(task.taskDeclaration.Out)
				for _, outPath := range outputPaths {
					taskLookup[outPath] = task
				}
			}
		}
	}

	// Process task references and convert them to proper dependencies
	processTaskReferences(taskLookup, taskDependencies)

	// Add implicit dependencies based on input/output file matching
	addImplicitDependencies(taskLookup, taskDependencies)

	// populate the dependencies
	for targetIdentifier := range taskDependencies {
		task := taskLookup[targetIdentifier]

		topSortedDependencies, err := topSortDependencies(taskDependencies, targetIdentifier)
		if err != nil {
			// Cycle detected - this is expected for cyclic tasks
			// The cycle will be handled by execution state tracking during runTask
			trace(fmt.Sprintf("Cycle detected for task %s: %v", targetIdentifier, err))

			// For cyclic tasks, add dependencies in the order they appear in the taskDependencies map
			// The execution state tracking will prevent infinite loops
			for _, depName := range taskDependencies[targetIdentifier] {
				depTask := taskLookup[depName]
				if depTask != nil {
					if depTask.taskDeclaration == nil {
						bailOnError(fmt.Errorf("subtask %s has no definition!?", depName))
					}
					task.taskDependencies = append(task.taskDependencies, depTask)
				}
			}
		} else {
			// No cycle - use topologically sorted order
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
	}

	parsedFlowDefinition := ParsedFlowDefinition{
		taskLookup:       taskLookup,
		taskDependencies: taskDependencies,
		executionEnv:     executionEnv,
	}
	return &parsedFlowDefinition
}

// processTaskReferences detects and processes task references in inputs
func processTaskReferences(taskLookup map[string]*RunnableTask, taskDependencies map[string][]string) {
	for taskName, task := range taskLookup {
		if task.taskDeclaration == nil {
			continue
		}

		// Only process tasks that are actual task names, not output file aliases
		if _, isRealTask := taskDependencies[taskName]; !isRealTask {
			continue
		}

		for _, input := range task.inputs {
			if isTaskDependency(input.originalInput, taskLookup) {
				// Mark this input as a task reference
				input.taskReference = input.originalInput

				// Mark the referenced task as being referenced
				if referencedTask, exists := taskLookup[input.originalInput]; exists {
					referencedTask.isReferenced = true
				}

				// The path will be resolved later during execution
				// For now, keep the original task name as the path

				// Add dependency (this was already being done by addImplicitDependencies,
				// but we'll do it here explicitly for task references)
				dependencyExists := false
				for _, existingDep := range taskDependencies[taskName] {
					if existingDep == input.originalInput {
						dependencyExists = true
						break
					}
				}

				if !dependencyExists {
					taskDependencies[taskName] = append(taskDependencies[taskName], input.originalInput)
				}
			}
		}
	}
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
			// Normalize input path for comparison
			inputPathAbs, err := filepath.Abs(input.path)
			if err != nil {
				inputPathAbs = input.path
			}

			// Check if input path matches any task's output file
			// Try both the original path and the absolute path
			var producingTask *RunnableTask
			var found bool

			if producingTask, found = taskLookup[input.path]; !found {
				producingTask, found = taskLookup[inputPathAbs]
			}

			if found && producingTask != nil {
				if producingTask.taskDeclaration != nil && producingTask.taskDeclaration.Out != nil {
					// Check if input matches any of the task's outputs
					outputPaths := getOutputPaths(producingTask.taskDeclaration.Out)
					matchFound := false
					for _, outPath := range outputPaths {
						outPathAbs, err := filepath.Abs(outPath)
						if err != nil {
							outPathAbs = outPath
						}

						// Match if paths are equal (after normalization)
						if outPath == input.path || outPathAbs == inputPathAbs {
							matchFound = true
							break
						}
					}

					if matchFound {
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
			}

			// Check for task references (any task name)
			if isTaskDependency(input.path, taskLookup) {
				producingTaskName := input.path

				// Check if this dependency already exists to avoid duplicates
				dependencyExists := false
				for _, existingDep := range taskDependencies[taskName] {
					if existingDep == producingTaskName {
						dependencyExists = true
						break
					}
				}

				if !dependencyExists {
					// Add task reference dependency
					taskDependencies[taskName] = append(taskDependencies[taskName], producingTaskName)
				}
			}
		}
	}
}

func runFlowDefinitionProcessor(flowDefinitionFilePath string, executor Executor, args []string) {

	parsedFlowDefinition := parseFlowDefinitionFile(flowDefinitionFilePath)

	if len(args) == 0 {
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
			printVitalsForTask(task, parsedFlowDefinition.taskLookup)
		}
	} else if len(args) > 0 {
		lastArg := args[len(args)-1]
		// see if lastarg is in our lookup
		if _, ok := parsedFlowDefinition.taskLookup[lastArg]; !ok {
			fmt.Printf("Task %s not found\n", lastArg)
			return
		} else {
			task := parsedFlowDefinition.taskLookup[lastArg]

			// Use parallel execution if more than 1 worker
			if executor.WorkerCount() > 1 {
				runTaskParallel(task, parsedFlowDefinition.executionEnv, executor, parsedFlowDefinition.taskLookup)
			} else {
				runTask(task, parsedFlowDefinition.executionEnv, executor, parsedFlowDefinition.taskLookup)
			}

			// Handle SHA256 updates for both explicit out: fields and CAS-captured stdout
			if executor.ShouldUpdateSha256() {
				var needsUpdate bool
				var sha256ToUpdate string

				if task.taskDeclaration.OutSha256 != nil {
					// Traditional case: task has explicit out: field
					// For single output, extract the SHA256 string
					if shaStr, ok := task.taskDeclaration.OutSha256.(string); ok {
						needsUpdate = true
						sha256ToUpdate = shaStr
					}
				} else if task.taskDeclaration.Out == nil {
					// New CAS case: task has no out: field, check if stdout was captured
					if contentDigest, ok := getTaskStdoutDigestFromCAS(task, executor); ok {
						needsUpdate = true
						sha256ToUpdate = string(contentDigest)
						// Set the OutSha256 in the task declaration so updateOutSha256ForTarget can find it
						task.taskDeclaration.OutSha256 = sha256ToUpdate
					}
				}

				if needsUpdate {
					updatedYamlString := updateOutSha256ForTarget(FLOW_DEFINITION_FILE, task.targetKey, sha256ToUpdate)
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

// Helper function for testing job flag parsing
func parseJobsFlag(args []string) (int, error) {
	for i, arg := range args {
		if arg == "-j" && i+1 < len(args) {
			jobCount := 0
			if _, err := fmt.Sscanf(args[i+1], "%d", &jobCount); err != nil {
				return 0, fmt.Errorf("invalid job count: %s", args[i+1])
			}
			if jobCount <= 0 {
				return 0, fmt.Errorf("job count must be positive, got: %d", jobCount)
			}
			return jobCount, nil
		}
	}
	return 1, nil // default
}

// Parallel execution functions

// calculateDependencyLevels computes the maximum dependency depth for each task
func calculateDependencyLevels(taskDependencies map[string][]string) map[string]int {
	levels := make(map[string]int)

	// Initialize all tasks to level -1 (unprocessed)
	for task := range taskDependencies {
		levels[task] = -1
	}

	// Calculate levels using DFS
	var calculateLevel func(task string) int
	calculateLevel = func(task string) int {
		if level, exists := levels[task]; exists && level >= 0 {
			return level // Already calculated
		}

		maxDepLevel := -1
		deps := taskDependencies[task]
		for _, dep := range deps {
			depLevel := calculateLevel(dep)
			if depLevel > maxDepLevel {
				maxDepLevel = depLevel
			}
		}

		levels[task] = maxDepLevel + 1
		return levels[task]
	}

	// Calculate level for all tasks
	for task := range taskDependencies {
		calculateLevel(task)
	}

	return levels
}

// groupTasksByLevel groups tasks by their dependency level
func groupTasksByLevel(levels map[string]int) map[int][]string {
	tasksByLevel := make(map[int][]string)

	for task, level := range levels {
		tasksByLevel[level] = append(tasksByLevel[level], task)
	}

	return tasksByLevel
}

// runTaskParallel is the parallel version of runTask
func runTaskParallel(task *RunnableTask, env map[string][]string, executor Executor, taskLookup map[string]*RunnableTask) {
	workerCount := executor.WorkerCount()

	// If single worker, just use sequential execution
	if workerCount == 1 {
		runTask(task, env, executor, taskLookup)
		return
	}

	// Build dependency graph for all tasks that this task depends on
	allTasks := collectAllTaskDependencies(task)
	if len(allTasks) == 1 {
		// Only this task, no dependencies - just run it
		runTask(task, env, executor, taskLookup)
		return
	}

	// Create task dependency map
	taskDeps := make(map[string][]string)

	for _, t := range allTasks {
		var deps []string
		for _, dep := range t.taskDependencies {
			deps = append(deps, dep.targetName)
		}
		taskDeps[t.targetName] = deps
	}

	// Calculate dependency levels
	levels := calculateDependencyLevels(taskDeps)
	tasksByLevel := groupTasksByLevel(levels)

	// Execute tasks level by level with parallelism within each level
	executeTasksByLevel(tasksByLevel, taskLookup, env, executor)
}

// collectAllTaskDependencies recursively collects all tasks that the given task depends on (including itself)
func collectAllTaskDependencies(task *RunnableTask) []*RunnableTask {
	visited := make(map[string]bool)
	var result []*RunnableTask

	var collect func(*RunnableTask)
	collect = func(t *RunnableTask) {
		if visited[t.targetName] {
			return
		}
		visited[t.targetName] = true

		// Collect dependencies first
		for _, dep := range t.taskDependencies {
			collect(dep)
		}

		result = append(result, t)
	}

	collect(task)
	return result
}

// executeTasksByLevel executes tasks level by level with parallelism within each level
func executeTasksByLevel(tasksByLevel map[int][]string, taskLookup map[string]*RunnableTask, env map[string][]string, executor Executor) {
	workerCount := executor.WorkerCount()

	// Find max level
	maxLevel := 0
	for level := range tasksByLevel {
		if level > maxLevel {
			maxLevel = level
		}
	}

	// Execute each level
	for level := 0; level <= maxLevel; level++ {
		tasksAtLevel := tasksByLevel[level]
		if len(tasksAtLevel) == 0 {
			continue
		}

		// Create worker pool for this level
		taskQueue := make(chan *RunnableTask, len(tasksAtLevel))
		var wg sync.WaitGroup

		// Start workers (up to workerCount or number of tasks, whichever is smaller)
		numWorkers := workerCount
		if len(tasksAtLevel) < numWorkers {
			numWorkers = len(tasksAtLevel)
		}

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range taskQueue {
					// Execute single task (without recursive dependency calls since we handle deps at level)
					runTaskSingle(task, env, executor, taskLookup)
				}
			}()
		}

		// Queue all tasks for this level
		for _, taskName := range tasksAtLevel {
			if task, exists := taskLookup[taskName]; exists {
				taskQueue <- task
			}
		}

		close(taskQueue)
		wg.Wait() // Wait for all tasks at this level to complete
	}
}

// runTaskSingle executes a single task without recursive dependency handling
func runTaskSingle(task *RunnableTask, env map[string][]string, executor Executor, taskLookup map[string]*RunnableTask) {
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

	executor.ShowTaskStart(task, taskLookup)

	// Note: We don't run dependencies here as they're handled at the level-by-level execution

	if task.taskDeclaration == nil {
		fmt.Println(color.RedString("No task declaration found!!!"))
		return
	}

	// Execute the task (copy relevant logic from original runTask but without dependency recursion)
	if task.taskDeclaration.In != nil && len(task.inputs) > 0 {
		// Handle input verification, downloads, etc.
		if task.taskDeclaration.InSha256 != nil {
			trace("Checking sha256 of input file")
			for _, input := range task.inputs {
				if handleRemoteInput(task, input) {
					continue
				}
				if input.sha256 != "" && isFileBytesMatchingSha256(input.path, input.sha256) {
					trace(fmt.Sprintf("IN SHA256 matches for %s", input.path))
				} else {
					trace(fmt.Sprintf("IN SHA256 mismatch for %s", input.path))
				}
			}
		}
	}

	// Execute the command if present
	if task.taskDeclaration.Run != nil {
		command, cmdEnv, err := renderCommandWithEnv(task, env)
		if err != nil {
			fmt.Printf("Error rendering command for task %s: %v\n", task.targetName, err)
			return
		}

		err = executor.ExecuteCommand(task, command, cmdEnv)
		if err != nil {
			fmt.Printf("Error executing command: %v\n", err)
			return
		}
	}

	// Handle SHA256 updates for referenced tasks (automatic caching) or when --updatehash is used
	if executor.ShouldUpdateSha256() || task.isReferenced {
		var needsUpdate bool
		var sha256ToUpdate string

		if task.taskDeclaration.OutSha256 != nil {
			// Traditional case: task has explicit out: field
			// For single output, extract the SHA256 string
			if shaStr, ok := task.taskDeclaration.OutSha256.(string); ok {
				needsUpdate = true
				sha256ToUpdate = shaStr
			}
		} else if task.taskDeclaration.Out == nil {
			// New CAS case: task has no out: field, check if stdout was captured
			if contentDigest, ok := getTaskStdoutDigestFromCAS(task, executor); ok {
				needsUpdate = true
				sha256ToUpdate = string(contentDigest)
				// Set the OutSha256 in the task declaration so updateOutSha256ForTarget can find it
				task.taskDeclaration.OutSha256 = sha256ToUpdate
			}
		}

		if needsUpdate {
			// Use mutex to prevent race conditions when multiple parallel tasks update YAML
			yamlUpdateMutex.Lock()
			defer yamlUpdateMutex.Unlock()

			updatedYamlString := updateOutSha256ForTarget(FLOW_DEFINITION_FILE, task.targetKey, sha256ToUpdate)
			outputYamlString := addInterveningSpacesToRootLevelBlocks(updatedYamlString)
			// re-output the file
			currentFileMode := os.ModePerm
			if fileInfo, err := os.Stat(FLOW_DEFINITION_FILE); err == nil {
				currentFileMode = fileInfo.Mode()
			}
			err := os.WriteFile(FLOW_DEFINITION_FILE, []byte(outputYamlString), currentFileMode)
			bailOnError(err)
			trace(fmt.Sprintf("Updated YAML file with new SHA256: %s", sha256ToUpdate))
		}
	}

	task.executionState = TaskCompleted
	task.executionCount++
	executor.ShowTaskCompleted(task)
}

func main() {
	COLORIZED_PROGRAM_NAME := color.HiBlueString(os.Args[0])
	// Defer flow definition file discovery until we know we need it
	if os.Getenv("SDFLOW_CACHE_DIRECTORY") != "" {
		CACHE_DIRECTORY = os.Getenv("SDFLOW_CACHE_DIRECTORY")
	}

	var rootCmd = &cobra.Command{

		Use:   fmt.Sprintf("%s %s", COLORIZED_PROGRAM_NAME, color.CyanString("[flags]")),
		Short: "flow runner",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Handle standalone commands that don't require Sdflow.yaml discovery
			versionFlag, _ := cmd.Flags().GetBool("version")
			if versionFlag {
				fmt.Println(version)
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

			// Now we know we need a flow definition file, so discover or use the provided one
			fileFlag, _ := cmd.Flags().GetString("file")
			if fileFlag != "" {
				FLOW_DEFINITION_FILE = fileFlag
			} else {
				FLOW_DEFINITION_FILE = discoverFlowDefinitionFile()
			}

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

			shouldUpdateOutSha256, _ := cmd.Flags().GetBool("updatehash")
			shouldForceRun, _ := cmd.Flags().GetBool("always-run")
			isDryRun, _ := cmd.Flags().GetBool("dry-run")
			jobCount, _ := cmd.Flags().GetInt("jobs")

			// Validate job count
			if jobCount <= 0 {
				return fmt.Errorf("invalid job count %d: must be positive", jobCount)
			}
			if jobCount > 32 {
				return fmt.Errorf("invalid job count %d: maximum allowed is 32", jobCount)
			}
			// TODO FIXME: tell user that updating the hash is meaningless if `out` is not supplied

			var executor Executor
			if isDryRun {
				executor = NewDryRunExecutorWithWorkers(jobCount, shouldUpdateOutSha256, shouldForceRun)
			} else {
				executor = NewRealExecutorWithWorkers(jobCount, shouldUpdateOutSha256, shouldForceRun)
			}

			runFlowDefinitionProcessor(FLOW_DEFINITION_FILE, executor, args)
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
	rootCmd.Flags().IntP("jobs", "j", 1, "number of parallel jobs (make-style syntax)")
	rootCmd.Flags().StringP("file", "f", "", "specify flow definition file (default: auto-discover {Sdflow,sdflow}.{yaml,yml,jsonnet,json})")
	rootCmd.Flags().BoolP("version", "v", false, "show version information")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
