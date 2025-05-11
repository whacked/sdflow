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

`sdflow`

by default, it looks for a file called `Sdflow.yaml` in the current working directory

```
    ╭─❮❮ build ❯❯
    ├ main.go
    ├ runnable.go
    ├ util.go
    ╰─▶ build/bin/sdflow (stale)

    ╭─❮❮ example-s3-loader ❯❯
    ├ s3://answer-reformulation-pds/README.txt
    ╰─▶ <STDOUT> (always)

    ...
```

see the included file for an example, which also includes the build command for itself

