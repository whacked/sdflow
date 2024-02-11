package main

import (
	"fmt"
	"testing"
)

const testConf string = `
env:
  BASE_PATH: s3://mybucket/myproject

myproject:
  out: ${BASE_PATH}/myoutput.jsonl
  in: ${BASE_PATH}/myinputs/project1/myinput.jsonl
  pre: setup-environment.sh
  run: jdxd --input ${in} --output ${out}
  post: cleanup.sh
  config:
    s3:
      retry: 3
    notify:
      failure:
        slach:
          message: "Job Failed: ${out}"
          channel: mychannel
`

func TestMain(t *testing.T) {
	t.Run("Hello", func(t *testing.T) {
		fmt.Println("Hello")
	})

	t.Run("Flake", func(t *testing.T) {
		fmt.Print(testConf)
	})
}
