package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/stevenle/topsort"
)

func bailOnError(err error) {
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func topSortDependencies(taskDependencies map[string][]string, targetTask string) []string {
	graph := topsort.NewGraph()

	for task, deps := range taskDependencies {
		for _, dep := range deps {
			graph.AddEdge(task, dep)
		}
	}

	sorted, err := graph.TopSort(targetTask)
	bailOnError(err)
	return sorted
}

func isPath(s string) bool {
	return strings.HasPrefix(s, "./") || strings.HasPrefix(s, "/")
}

func isRemotePath(s string) bool {
	return strings.HasPrefix(s, "http") || strings.HasPrefix(s, "s3://")
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

	awsSession := session.Must(session.NewSession(
		&aws.Config{
			Region: aws.String(awsRegion),
		},
	))

	downloader := s3manager.NewDownloader(awsSession)
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err = downloader.Download(buf, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(pathInBucket),
	})

	if err != nil {
		fmt.Fprintf(
			os.Stderr,
			"failed to download file from S3: %v",
			err,
		)
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

func sample3() {
	taskDependencies := map[string][]string{
		"task1": {},
		"task2": {"task1"},
		"task3": {"task2"},
	}

	topSortedDependencies := topSortDependencies(taskDependencies, "task3")
	for _, task := range topSortedDependencies {
		fmt.Println(task)
	}
}
