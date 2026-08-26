SHELL := /bin/sh
GO ?= go
BIN_DIR ?= bin
ARTIFACT_DIR ?= artifacts

.PHONY: all build build-server build-workbench test test-race test-integration verify fmt-check vet clean

all: verify build

build: build-server build-workbench

build-server:
	@mkdir -p "$(BIN_DIR)"
	$(GO) build -trimpath -o "$(BIN_DIR)/qmax" ./cmd/qmax

build-workbench:
	@mkdir -p "$(BIN_DIR)"
	$(GO) build -trimpath -o "$(BIN_DIR)/qmax-workbench" ./cmd/qmax-workbench

test:
	@mkdir -p "$(ARTIFACT_DIR)"
	$(GO) test ./... -count=1 -shuffle=on -coverprofile="$(ARTIFACT_DIR)/coverage.out"

test-race:
	$(GO) test ./... -race -count=1

test-integration:
	$(GO) test ./internal/integration -count=1 -v

fmt-check:
	@test -z "$$(gofmt -l .)"

vet:
	$(GO) vet ./...

verify:
	ARTIFACT_DIR="$(ARTIFACT_DIR)" ./scripts/verify.sh

clean:
	rm -rf "$(BIN_DIR)" "$(ARTIFACT_DIR)"
