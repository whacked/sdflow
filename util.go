package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"

	yaml "gopkg.in/yaml.v3"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stevenle/topsort"
)

var debugLevel int

// Global mutex for thread-safe YAML file updates in parallel execution
var yamlUpdateMutex sync.Mutex

func init() {
	debugLevelStr := os.Getenv("DEBUG_LEVEL")
	if debugLevelStr != "" {
		fmt.Sscanf(debugLevelStr, "%d", &debugLevel)
	}
}

func bailOnError(err error) {
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func trace(msg string) {
	if debugLevel > 0 {
		fmt.Fprintf(os.Stderr, "[TRACE] %s\n", msg)
	}
}

func topSortDependencies(taskDependencies map[string][]string, targetTask string) ([]string, error) {
	/*
		taskDependencies := map[string][]string{
			"task1": {},
			"task2": {"task1"},
			"task3": {"task2"},
		}

		topSortedDependencies, err := topSortDependencies(taskDependencies, "task3")
		for _, task := range topSortedDependencies {
			fmt.Println(task)
		}
		> task1
		> task2
		> task3
	*/
	graph := topsort.NewGraph()

	for task, deps := range taskDependencies {
		for _, dep := range deps {
			graph.AddEdge(task, dep)
		}
	}

	sorted, err := graph.TopSort(targetTask)
	return sorted, err
}

// expandTilde expands ~ and ~/ prefixes in paths to the user's home directory
// This is the Go equivalent of Python's os.path.expanduser()
// Returns the original path if it doesn't start with ~ or if expansion fails
func expandTilde(path string) string {
	if path == "" || !strings.HasPrefix(path, "~") {
		return path
	}

	// Get the user's home directory
	home, err := os.UserHomeDir()
	if err != nil {
		// If we can't get home dir, return the original path unchanged
		trace(fmt.Sprintf("Warning: Failed to expand tilde in path '%s': %v", path, err))
		return path
	}

	// Handle "~" alone or "~/"
	if path == "~" {
		return home
	}
	if strings.HasPrefix(path, "~/") {
		return filepath.Join(home, path[2:])
	}

	// If it's ~username/path, we don't support that (would require user lookup)
	// Just return the original path
	return path
}

func isPath(s string) bool {
	return strings.HasPrefix(s, "./") || strings.HasPrefix(s, "../") || strings.HasPrefix(s, "/") || strings.HasPrefix(s, "~")
}

func isRemotePath(path string) bool {
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") || strings.HasPrefix(path, "s3://")
}

// yaml handling
func detectFirstIndentationLevel(yamlSource string) int {
	pattern := regexp.MustCompile(`^\s+[^#].*`)

	lines := strings.Split(yamlSource, "\n")
	for _, line := range lines {
		if pattern.MatchString(line) {
			return getIndentationLevel(line)
		}
	}
	return 2 // Default indentation level if none detected.
}

func getIndentationLevel(line string) int {
	return len(line) - len(strings.TrimLeft(line, " "))
}

func addInterveningSpacesToRootLevelBlocks(yamlSource string) string {
	/*
		adds a spacing newline after a line if:
		Rule 1: the line's indentation level > 0 AND the next line's indentation level == 0
		Rule 2: the line is not a comment AND its indentation level is 0 AND the next line is a comment
	*/

	lines := strings.Split(yamlSource, "\n")
	var processedLines []string

	var currentIndentation int = 0

	for i, line := range lines {
		isCurrentLineComment := strings.HasPrefix(strings.TrimSpace(line), "#")

		processedLines = append(processedLines, line)

		if i+1 < len(lines) {
			nextLine := lines[i+1]
			nextIndentation := getIndentationLevel(nextLine)
			nextLineIsComment := strings.HasPrefix(strings.TrimSpace(nextLine), "#")

			if currentIndentation > 0 && nextIndentation == 0 {
				// rule 1 matched
				processedLines = append(processedLines, "")
			} else if !isCurrentLineComment && currentIndentation == 0 && nextLineIsComment {
				// rule 2 matched
				processedLines = append(processedLines, "")
			}
			currentIndentation = nextIndentation
		}
	}

	return strings.TrimSpace(strings.Join(processedLines, "\n"))
}

func downloadRemoteFileFromHttp(url string) []byte {
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"failed to make a GET request: %v",
			err,
		)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(
			os.Stderr,
			"received non-200 response status: %d %s",
			resp.StatusCode,
			resp.Status,
		)
		return nil
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"failed to read response body: %v",
			err,
		)
	}

	return body
}

func downloadRemoteFileFromS3(s3Uri string) []byte {
	fmt.Fprintf(os.Stderr, "downloadRemoteFileFromS3 called with: %s\n", s3Uri)
	u, err := url.Parse(s3Uri)
	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"failed to parse S3 URI: %v",
			err,
		)
		return nil
	}

	if u.Scheme != "s3" {
		fmt.Fprintf(
			os.Stderr,
			"invalid URI scheme: %s",
			u.Scheme,
		)
		return nil
	}

	bucketName := u.Host
	pathInBucket := u.Path[1:] // Remove the leading slash

	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = os.Getenv("AWS_DEFAULT_REGION")
		if awsRegion == "" {
			awsRegion = "us-east-1"
		}
	}

	config := &aws.Config{Region: aws.String(awsRegion)}
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		config.Endpoint = aws.String(endpoint)
		config.S3ForcePathStyle = aws.Bool(true)
		if os.Getenv("AWS_S3_DISABLE_SSL") == "true" {
			config.DisableSSL = aws.Bool(true)
		}
	}
	awsSession := session.Must(session.NewSession(config))

	downloader := s3manager.NewDownloader(awsSession)
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err = downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(pathInBucket),
	})

	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to download file from S3: %v\n", err)
		fmt.Fprintf(os.Stderr, "Debug info:\n")
		fmt.Fprintf(os.Stderr, "  Bucket: %s\n", bucketName)
		fmt.Fprintf(os.Stderr, "  Key: %s\n", pathInBucket)
		fmt.Fprintf(os.Stderr, "  AWS_ENDPOINT_URL: %s\n", os.Getenv("AWS_ENDPOINT_URL"))
		fmt.Fprintf(os.Stderr, "  AWS_ACCESS_KEY_ID: %s\n", os.Getenv("AWS_ACCESS_KEY_ID"))
		fmt.Fprintf(os.Stderr, "  AWS_S3_FORCE_PATH_STYLE: %s\n", os.Getenv("AWS_S3_FORCE_PATH_STYLE"))
		fmt.Fprintf(os.Stderr, "  AWS_S3_DISABLE_SSL: %s\n", os.Getenv("AWS_S3_DISABLE_SSL"))
		fmt.Fprintf(os.Stderr, "  AWS_REGION: %s\n", awsRegion)
		return nil
	}

	return buf.Bytes()
}

func getRemoteResourceBytes(remoteResourceLocation string) []byte {
	if strings.HasPrefix(remoteResourceLocation, "http") {
		return downloadRemoteFileFromHttp(remoteResourceLocation)
	} else if strings.HasPrefix(remoteResourceLocation, "s3://") {
		return downloadRemoteFileFromS3(remoteResourceLocation)
	}
	return nil
}

func commandExists(cmd string) bool {
	_, err := exec.LookPath(cmd)
	return err == nil
}

func isValidAWSCLI(path string) bool {
	cmd := exec.Command(path, "--version")
	output, err := cmd.Output()
	if err != nil {
		return false
	}
	return strings.HasPrefix(string(output), "aws-cli/")
}

func isValidMinioClient(path string) bool {
	cmd := exec.Command(path, "--version")
	output, err := cmd.Output()
	if err != nil {
		return false
	}

	outputStr := strings.ToLower(string(output))
	return strings.Contains(outputStr, "minio") &&
		   !strings.Contains(outputStr, "midnight")
}

func detectValidS3Tools() (bool, bool) {
	awsPath, awsErr := exec.LookPath("aws")
	mcPath, mcErr := exec.LookPath("mc")

	awsExists := awsErr == nil
	mcExists := mcErr == nil

	validAWS := awsExists && isValidAWSCLI(awsPath)
	validMinio := mcExists && isValidMinioClient(mcPath)

	// Log what we found for debugging
	if mcExists && !validMinio {
		trace("found 'mc' command but it's not MinIO client (likely Midnight Commander)")
	}
	if awsExists && !validAWS {
		trace("found 'aws' command but version check failed")
	}

	return validAWS, validMinio
}

func downloadS3FileSimple(s3Uri string, outputPath *string) error {
	fmt.Fprintf(os.Stderr, "S3 URL detected. Checking download options...\n")

	awsExists, mcExists := detectValidS3Tools()
	awsKeyId := os.Getenv("AWS_ACCESS_KEY_ID")
	awsSecret := os.Getenv("AWS_SECRET_ACCESS_KEY")
	minioKey := os.Getenv("MINIO_ACCESS_KEY")
	minioSecret := os.Getenv("MINIO_SECRET_KEY")

	trace(fmt.Sprintf("valid aws CLI: %v, valid mc CLI: %v", awsExists, mcExists))
	trace(fmt.Sprintf("AWS_ACCESS_KEY_ID: %s, AWS_SECRET_ACCESS_KEY: %s, MINIO_ACCESS_KEY: %s, MINIO_SECRET_KEY: %s", awsKeyId, awsSecret, minioKey, minioSecret))

	if awsExists && awsKeyId != "" && awsSecret != "" {
		trace("downloading using aws")
		if outputPath != nil {
			return exec.Command("aws", "s3", "cp", s3Uri, *outputPath).Run()
		} else {
			return exec.Command("aws", "s3", "cp", s3Uri, "-").Run()
		}
	} else if mcExists && ((minioKey != "" && minioSecret != "") || (awsKeyId != "" && awsSecret != "")) {
		trace("downloading using mc")
		// Use MinIO credentials if available, otherwise fall back to AWS credentials
		accessKey := minioKey
		secretKey := minioSecret
		if accessKey == "" || secretKey == "" {
			accessKey = awsKeyId
			secretKey = awsSecret
		}
		return downloadUsingMinioClient(s3Uri, outputPath, accessKey, secretKey)
	} else {
		trace("using built-in s3 downloader")
		bytes := downloadRemoteFileFromS3(s3Uri)
		if len(bytes) > 0 {
			if outputPath != nil {
				fmt.Fprintf(os.Stderr, "Downloaded %d bytes, writing to %s\n", len(bytes), *outputPath)
				err := os.WriteFile(*outputPath, bytes, 0644)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Failed to write file: %v\n", err)
				}
				return err
			} else {
				_, err := os.Stdout.Write(bytes)
				return err
			}
		} else {
			return fmt.Errorf("built-in S3 downloader failed to retrieve file")
		}
	}
}

func downloadFileToLocalPath(url, outputPath string) error {
	fmt.Fprintf(os.Stderr, "downloadFileToLocalPath called with: %s -> %s\n", url, outputPath)
	if strings.HasPrefix(url, "http") {
		if commandExists("curl") {
			trace("downloading using curl")
			return exec.Command("curl", "-o", outputPath, url).Run()
		} else if commandExists("wget") {
			trace("downloading using wget")
			return exec.Command("wget", "-O", outputPath, url).Run()
		} else {
			trace("downloading using built-in http downloader")
			bytes := downloadRemoteFileFromHttp(url)
			return os.WriteFile(outputPath, bytes, 0644)
		}
	} else if strings.HasPrefix(url, "s3://") {
		return downloadS3FileSimple(url, &outputPath)
	}
	return nil
}

func downloadFileToStdout(url string) error {
	fmt.Fprintf(os.Stderr, "downloadFileToStdout called with: %s\n", url)

	if strings.HasPrefix(url, "http://") || strings.HasPrefix(url, "https://") {
		response, err := http.Get(url)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != 200 {
			return fmt.Errorf("unexpected HTTP status: %d", response.StatusCode)
		}

		_, err = io.Copy(os.Stdout, response.Body)
		return err
	} else if strings.HasPrefix(url, "s3://") {
		return downloadS3FileSimple(url, nil)
	}
	return nil
}

func downloadUsingMinioClient(s3Uri string, outputPath *string, accessKey, secretKey string) error {
	// Parse S3 URL to extract bucket and path
	u, err := url.Parse(s3Uri)
	if err != nil {
		return fmt.Errorf("failed to parse S3 URI: %v", err)
	}

	if u.Scheme != "s3" {
		return fmt.Errorf("invalid URI scheme: %s", u.Scheme)
	}

	bucketAndPath := u.Host + u.Path

	// Get endpoint from environment
	endpoint := os.Getenv("AWS_ENDPOINT_URL")
	if endpoint == "" {
		return fmt.Errorf("AWS_ENDPOINT_URL not set for MinIO client")
	}

	// Create MC_HOST environment variable
	// Format: http://accesskey:secretkey@host:port
	mcHostValue := fmt.Sprintf("http://%s:%s@%s",
		url.QueryEscape(accessKey),
		url.QueryEscape(secretKey),
		strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"))

	// Construct mc command - use cp for files, cat for stdout
	mcPath := fmt.Sprintf("sdflowtemps3server/%s", bucketAndPath)
	var cmd *exec.Cmd
	if outputPath != nil {
		cmd = exec.Command("mc", "cp", mcPath, *outputPath)
	} else {
		cmd = exec.Command("mc", "cat", mcPath)
		cmd.Stdout = os.Stdout
	}

	// Set environment for the command
	cmd.Env = append(os.Environ(), fmt.Sprintf("MC_HOST_sdflowtemps3server=%s", mcHostValue))

	return cmd.Run()
}

func getBytesSha256(bytes []byte) string {
	bytesSha256 := sha256.Sum256(bytes)
	return hex.EncodeToString(bytesSha256[:])
}

func isBytesMatchingSha256(bytes []byte, precomputedSha256 string) bool {
	return getBytesSha256(bytes) == precomputedSha256
}

func isFileBytesMatchingSha256(filePath string, precomputedSha256 string) bool {
	trace(fmt.Sprintf("checking sha256 of file: %s", filePath))
	fileBytes, err := os.ReadFile(filePath)
	bailOnError(err)
	return isBytesMatchingSha256(fileBytes, precomputedSha256)
}

func updateOutSha256ForTarget(flowDefinitionFile string, targetKey string, newSha256 string) string {
	flowDefinitionFileSource, err := os.ReadFile(flowDefinitionFile)
	bailOnError(err)
	originalIndentationLevel := detectFirstIndentationLevel(string(flowDefinitionFileSource))

	var node yaml.Node
	err = yaml.Unmarshal(flowDefinitionFileSource, &node)
	bailOnError(err)

	updateSHA256InEntry(&node, targetKey, "out.sha256", newSha256)

	outputBuffer := &bytes.Buffer{}
	yamlEncoder := yaml.NewEncoder(outputBuffer)
	yamlEncoder.SetIndent(originalIndentationLevel)

	if err := yamlEncoder.Encode(node.Content[0]); err != nil {
		log.Fatalf("Marshalling failed %s", err)
	}
	yamlEncoder.Close()
	return string(outputBuffer.String())
}

func updateSHA256InEntry(node *yaml.Node, targetKey, propName string, newValue string) {
	for _, n := range node.Content {
		if n.Kind == yaml.MappingNode {
			for i := 0; i < len(n.Content); i += 2 {
				keyNode := n.Content[i]
				if keyNode.Value == targetKey {
					// Found the entry, now find or add the `out.sha256` within this entry
					valNode := n.Content[i+1]
					if valNode.Kind == yaml.MappingNode {
						updateOrAddKey(valNode, propName, newValue)
					}
					return
				}
			}
		}
	}
}

func updateOrAddKey(node *yaml.Node, key, newValue string) {
	found := false
	for i := 0; i < len(node.Content); i += 2 {
		keyNode := node.Content[i]
		if keyNode.Value == key {
			// Update the existing value
			node.Content[i+1].Value = newValue
			found = true
			break
		}
	}

	if !found {
		// Key not found, add it
		node.Content = append(node.Content, &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: key,
		}, &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: newValue,
		})
	}
}

func getOsEnvironAsMap() map[string]string {
	environ := make(map[string]string)

	// add keyvals from environ to executionEnv
	for _, envVar := range os.Environ() {
		parts := strings.SplitN(envVar, "=", 2)
		environ[parts[0]] = parts[1]
	}
	return environ
}

func convertEnvironToArrayMap(environ map[string]string) map[string][]string {
	arrayEnv := make(map[string][]string)
	for key, value := range environ {
		arrayEnv[key] = []string{value}
	}
	return arrayEnv
}

func prepareExpandableTemplate(template string, executionEnv map[string][]string) string {
	// Replace ${VAR[i]} syntax with ${VAR_i} for os.Expand compatibility
	re := regexp.MustCompile(`\$\{([^}]+)\[(\d+)\]\}`)
	return re.ReplaceAllStringFunc(template, func(match string) string {
		parts := re.FindStringSubmatch(match)
		if len(parts) != 3 {
			return match
		}
		
		varName := parts[1]
		indexStr := parts[2]
		index, err := strconv.Atoi(indexStr)
		if err != nil {
			return match
		}
		
		// Check if the variable exists and index is valid
		if values, exists := executionEnv[varName]; exists && index < len(values) {
			return fmt.Sprintf("${%s_%d}", varName, index)
		}
		
		return match
	})
}

// expandSdflowBuiltins performs controlled substitution of only sdflow builtin variables.
// This replaces the aggressive os.Expand approach with targeted substitution that only
// handles: $in, $out, ${in}, ${out}, ${in[N]}, and ${in.ALIAS}
// All other variables (including YAML globals and shell variables) are left untouched.
// Returns an error if builtin variable references are invalid (e.g., out of range index or undefined alias).
// createFieldHandlers creates a set of pattern handlers for a field (in or out)
// This eliminates duplication between input and output expansion logic
func createFieldHandlers(fieldName string, taskEnv map[string][]string) []struct {
	regex   *regexp.Regexp
	handler func(match string, groups []string) (string, error)
} {
	return []struct {
		regex   *regexp.Regexp
		handler func(match string, groups []string) (string, error)
	}{
		// ${field[N]} - indexed access (throws on out of range)
		{
			regex: regexp.MustCompile(fmt.Sprintf(`\$\{%s\[(\d+)\]\}`, fieldName)),
			handler: func(match string, groups []string) (string, error) {
				if len(groups) < 2 {
					return "", fmt.Errorf("invalid indexed %s syntax: %s", fieldName, match)
				}
				index, err := strconv.Atoi(groups[1])
				if err != nil {
					return "", fmt.Errorf("invalid index in %s: %v", match, err)
				}
				values, exists := taskEnv[fieldName]
				if !exists {
					return match, nil // Leave untouched if field not defined
				}
				if index >= len(values) {
					return "", fmt.Errorf("index %d out of range for %s array (length %d)", index, fieldName, len(values))
				}
				return values[index], nil
			},
		},
		// ${field.N} - dot-notation indexed access
		{
			regex: regexp.MustCompile(fmt.Sprintf(`\$\{%s\.(\d+)\}`, fieldName)),
			handler: func(match string, groups []string) (string, error) {
				if len(groups) < 2 {
					return "", fmt.Errorf("invalid dot-indexed %s syntax: %s", fieldName, match)
				}
				index, err := strconv.Atoi(groups[1])
				if err != nil {
					return "", fmt.Errorf("invalid index in %s: %v", match, err)
				}
				values, exists := taskEnv[fieldName]
				if !exists {
					return match, nil // Leave untouched if field not defined
				}
				if index >= len(values) {
					return "", fmt.Errorf("index %d out of range for %s array (length %d)", index, fieldName, len(values))
				}
				return values[index], nil
			},
		},
		// ${field.ALIAS} - aliased/mapped access (throws on undefined alias)
		{
			regex: regexp.MustCompile(fmt.Sprintf(`\$\{%s\.([a-zA-Z_][a-zA-Z0-9_]*)\}`, fieldName)),
			handler: func(match string, groups []string) (string, error) {
				if len(groups) < 2 {
					return "", fmt.Errorf("invalid aliased %s syntax: %s", fieldName, match)
				}
				alias := groups[1]
				// Check if this is a numeric alias (handled by dot-notation pattern above)
				if _, err := strconv.Atoi(alias); err == nil {
					return match, nil // Let the numeric pattern handle it
				}
				aliasKey := fmt.Sprintf("%s.%s", fieldName, alias)
				if aliasValues, exists := taskEnv[aliasKey]; exists && len(aliasValues) > 0 {
					return aliasValues[0], nil
				}
				return "", fmt.Errorf("undefined %s alias: %s", fieldName, alias)
			},
		},
		// ${field} - all values as space-separated string
		{
			regex: regexp.MustCompile(fmt.Sprintf(`\$\{%s\}`, fieldName)),
			handler: func(match string, groups []string) (string, error) {
				if values, exists := taskEnv[fieldName]; exists {
					return strings.Join(values, " "), nil
				}
				return match, nil // Leave untouched if field not defined
			},
		},
		// $field - unbraced version, all values space-separated
		{
			regex: regexp.MustCompile(fmt.Sprintf(`\$%s\b`, fieldName)),
			handler: func(match string, groups []string) (string, error) {
				if values, exists := taskEnv[fieldName]; exists {
					return strings.Join(values, " "), nil
				}
				return match, nil // Leave untouched if field not defined
			},
		},
	}
}

func expandSdflowBuiltins(template string, taskEnv map[string][]string) (string, error) {
	// Handle $$ escape sequences by temporarily replacing them, but only for builtins
	// Since we don't substitute non-builtins anymore, we only need to escape builtin patterns
	const placeholder = "___ESCAPED_DOLLAR___"

	// First, escape $$ patterns that precede builtin variable names
	builtinEscapePattern := regexp.MustCompile(`\$\$(in|out)\b`)
	result := builtinEscapePattern.ReplaceAllString(template, placeholder+"$1")

	// Also escape braced builtin patterns
	bracedEscapePattern := regexp.MustCompile(`\$\$\{(in|out)([^}]*)\}`)
	result = bracedEscapePattern.ReplaceAllString(result, placeholder+"{$1$2}")

	// Create handlers for both 'in' and 'out' fields using the generic factory
	// This ensures complete parity between input and output expansion semantics
	var patterns []struct {
		regex   *regexp.Regexp
		handler func(match string, groups []string) (string, error)
	}

	// Add input handlers: ${in[0]}, ${in.key}, ${in}, $in
	patterns = append(patterns, createFieldHandlers("in", taskEnv)...)

	// Add output handlers: ${out[0]}, ${out.key}, ${out}, $out
	patterns = append(patterns, createFieldHandlers("out", taskEnv)...)

	// Apply each pattern
	for _, pattern := range patterns {
		var err error
		result = pattern.regex.ReplaceAllStringFunc(result, func(match string) string {
			groups := pattern.regex.FindStringSubmatch(match)
			replacement, handlerErr := pattern.handler(match, groups)
			if handlerErr != nil {
				err = handlerErr
			}
			return replacement
		})
		if err != nil {
			return "", err
		}
	}

	// Restore escaped dollars (only for builtin patterns we actually escaped)
	result = strings.ReplaceAll(result, placeholder, "$")
	return result, nil
}

// Legacy function - kept for backward compatibility during transition
func expandVariables(template string, executionEnv map[string][]string) string {
	// Handle $$ escape sequences by temporarily replacing them
	const placeholder = "___ESCAPED_DOLLAR___"
	escapedTemplate := strings.ReplaceAll(template, "$$", placeholder)

	// First pass: prepare ${VAR[i]} syntax for os.Expand
	preparedTemplate := prepareExpandableTemplate(escapedTemplate, executionEnv)

	// Create a flat environment map for os.Expand
	flatEnv := make(map[string]string)
	for varName, values := range executionEnv {
		// Add array variable as space-separated string
		flatEnv[varName] = strings.Join(values, " ")

		// Add individual indexed variables (VAR_0, VAR_1, etc.)
		for i, value := range values {
			flatEnv[fmt.Sprintf("%s_%d", varName, i)] = value
		}
	}

	// Second pass: use os.Expand for standard ${VAR} syntax
	expanded := os.Expand(preparedTemplate, func(key string) string {
		if value, exists := flatEnv[key]; exists {
			return value
		}
		return ""
	})

	// Third pass: restore escaped dollars
	return strings.ReplaceAll(expanded, placeholder, "$")
}

func convertArrayMapToStringMap(arrayEnv map[string][]string) map[string]string {
	stringEnv := make(map[string]string)
	for key, values := range arrayEnv {
		if len(values) > 0 {
			// Use the first value for simple substitution
			stringEnv[key] = values[0]
		}
	}
	return stringEnv
}

func expandArrayVariableInInput(inputStr string, executionEnv map[string][]string) []string {
	// Check if the input string is a simple variable reference like "${varname}"
	re := regexp.MustCompile(`^\$\{([^}]+)\}$`)
	match := re.FindStringSubmatch(inputStr)
	
	if len(match) == 2 {
		varName := match[1]
		if values, exists := executionEnv[varName]; exists {
			if len(values) > 1 {
				// Multiple values - return the array
				return values
			} else if len(values) == 1 {
				// Single value - return as single-element array
				return []string{values[0]}
			}
		}
	}
	
	// Not a simple array variable reference - expand normally and return as single item
	expanded := expandVariables(inputStr, executionEnv)
	return []string{expanded}
}
