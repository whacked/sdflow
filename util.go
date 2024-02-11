package main

import (
	"fmt"
	"log"

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
