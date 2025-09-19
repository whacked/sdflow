# sdflow -- schematized/simple data flow runner

tiny in--{command}-->out runner

`sdflow` fits in a project that does small-scale, standardized, repeated data load and transformation.

if you are interested in `sdflow` you _probably_ should use consider other tools first:

- [make](https://www.gnu.org/software/make/), my go-to tool if I don't need built-in checksum
- [go-task](https://taskfile.dev/), too verbose for my taste
- [just](https://github.com/casey/just), also doesn't have built-in validation
- [dvc](https://dvc.org/), too cumbersome for repeat, small-scale runs
- [tup](https://gittup.org/tup/), uncommon syntax and overly strict for dependency declaration

sdflow puts these features together:

- simple task overview
- suitable for small number of targets, tiny syntax, simple dependency chains
- built-in sha256 validation or generation for inputs/outputs, so the Sdflow.yaml file serves as the reference for data integrity.
- built-in support for HTTP(S)/S3 sources as inputs

Anything else, use a more mature tool.

# design considerations

1. ergonomics

# usage

`sdflow [-f flowfile] [taskname]`

by default, it looks for a file called `Sdflow.yaml` (or `{sdflow,Sdflow}.{yml,jsonnet,json}`) in the current working directory.

command line help:

<!-- BEGIN_SDFLOW_HELP -->
```
flow runner

Usage:
  build/bin/sdflow [flags]

Flags:
  -B, --always-run           always run the target, even if it's up to date
      --completions string   get shell completion code for the given shell type
      --dry-run              show execution plan without running commands
  -f, --file string          specify flow definition file (default: auto-discover {Sdflow,sdflow}.{yaml,yml,jsonnet,json})
  -h, --help                 help for build/bin/sdflow
  -j, --jobs int             number of parallel jobs (make-style syntax) (default 1)
      --targets              list all defined targets
      --updatehash           update out.sha256 for the target in the flow definition file after running the target
      --validate             validate the flow definition file
  -v, --version              show version information
```
<!-- END_SDFLOW_HELP -->

## task definition

```yaml
build:
  in: # currently sha256 is only supported for single input
    - main.go
    - ./runnable.go
    - util.go
    - version.go
    - resources/bash_autocomplete.sh
    - resources/zsh_autocomplete.sh
  out: build/bin/sdflow
  # make dirs for nix build compat
  run: echo INPUTS; echo $in; mkdir -p $(dirname $out); go build -o $out ${in[0]} ${in[1]} ${in[2]} ${in[3]}; ln -sf ./build ./result
```

## example overview

```
╭─❮❮ build ❯❯
├ ./main.go
├ ./runnable.go
├ ./util.go
├ ./version.go
├ ./resources/bash_autocomplete.sh
├ ./resources/zsh_autocomplete.sh
├─◁ echo INPUTS; echo $in; mkdir -p $(dirname $out); go build -o $out ${in[0]} ${in[1]} ${in[2]} ${in[3]}; ln -sf ./build ./result
│
╰─▶ ./build/bin/sdflow (current)

```

see the included file for an example, which also includes the build command for itself
