.SILENT: ; # no need for @

PROJECT=s3cp
PROJECT_DIR=$(shell pwd)

GOFILES:=$(shell find . -name '*.go' -not -path './vendor/*')
GOPACKAGES:=$(shell go list ./... | grep -v /vendor/| grep -v /checkers)
OS := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)

WORKDIR:=$(PROJECT_DIR)/_workdir

default: build-linux

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(WORKDIR)/$(PROJECT)_linux_amd64 $(GO_BUILD_FLAGS)

build:
	echo	CGO_ENABLED=0 go build -o $(WORKDIR)/$(PROJECT)_$(OS)_$(ARCH) $(GO_BUILD_FLAGS)

clean:
	rm -f $(WORKDIR)/*
	rm -rf .cover
	go clean -r

coverage:
	./_misc/coverage.sh

coverage-html:
	./_misc/coverage.sh --html

dependencies:
	go get honnef.co/go/tools/cmd/megacheck
	go get github.com/alecthomas/gometalinter
	go get github.com/golang/dep/cmd/dep
	dep ensure
	gometalinter --install

develop: dependencies
	(cd .git/hooks && ln -sf ../../_misc/pre-push.bash pre-push )
	git flow init -d

lint:
	echo "metalinter..."
	gometalinter --enable=goimports --enable=unparam --enable=unused --disable=golint --disable=govet $(GOPACKAGES)
	echo "megacheck..."
	megacheck $(GOPACKAGES)
	echo "golint..."
	golint $(GOPACKAGES)
	echo "go vet..."
	go vet --all $(GOPACKAGES)

test:
	CGO_ENABLED=0 go test $(GOPACKAGES)

test-race:
	CGO_ENABLED=1 go test -race -coverprofile=coverage.txt -covermode=atomic $(GOPACKAGES)

