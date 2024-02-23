# make, schmake

tiny in--{command}-->out runner

you _probably_ should use something else:

- [make](https://www.gnu.org/software/make/)
- [go-task](https://taskfile.dev/)
- [just](https://github.com/casey/just)
- [dvc](https://dvc.org/)

schmake fits in a project that does small-scale, standardized, repeated data load and transformation.

I like `make`, but want an easier way of doing input/output hash validation.
`just` also doesn't have built-in validation.
`task` syntax is too verbose for these low-complexity targets.
`dvc` is too cumbersome for repeat runs.

schmake just puts these features together:

- suitable for small number of targets, tiny syntax, simple dependency chains
- built-in sha256 validation or generation for inputs/outputs, so the Schmakefile serves as the reference for data integrity.
- built-in support for HTTP(S)/S3 sources as inputs

Anything else, you should use a more mature tool.
