package main

import (
	"fmt"
	"log"
	"regexp"
	"strings"

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
