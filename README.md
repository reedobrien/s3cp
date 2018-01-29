# s3cp

[![CircleCI](https://circleci.com/gh/reedobrien/s3cp.svg?style=svg)](https://circleci.com/gh/reedobrien/s3cp)
[![Go Report Card](https://goreportcard.com/badge/github.com/reedobrien/s3cp)](https://goreportcard.com/report/github.com/reedobrien/s3cp) [![codecov](https://codecov.io/gh/reedobrien/s3cp/branch/master/graph/badge.svg)](https://codecov.io/gh/reedobrien/s3cp)

## S3 Copy Manager

### Development

Clone the repo, then in the cloned directory run:

`make develop`

Now you can create a feature branch using `git-flow`, or just create it your self from the development branch. Write some code and make a PR.

You can run `make test` or `make lint` or other make targets (see below) as desired. When you push it should run lint, build, and test-race for you. You may also invoke this by running `make run-push-hook`.

## Make targets

- build -  Builds the package for the host architecture.
- build-linux - Builds the package for linux amd64.
- clean - Cleans out $WORKDIR, .cover, and runs `go clean -r`.
- coverage - Runs the tests and sends coverage information to stdout.
- coverage-html  - Runs the tests with coverage and opens a browser view of coverage.
- default - Sets the default make target to build-linux if no target is supplied.
- dependencies - Installs the dependencies required, megacheck, metaliter, dep, etc...
- develop  - Calls dependencies then intitializes the pre-push hook and git-flow.
- lint - Runs static analysis the tools, `megacheck` and `metalinter`.
- run-push-hook - Runs the pre-push hook script in `_misc`.
- test - Runs the tests.
- test-race - Runs the tests with the race detector on.
