GO      ?= go
BINARY  := bin/ddl-saver

.PHONY: all build test cover fuzz test-e2e lint fmt tidy docker clean

all: build

build:
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags "-s -w" -o $(BINARY) ./cmd/ddl-saver

test:
	$(GO) test -race -count=1 -shuffle=on ./internal/... ./cmd/...

cover:
	$(GO) test -count=1 -coverprofile=coverage.out ./internal/... && $(GO) tool cover -func=coverage.out | tail -1

fuzz: ## 30s per target
	for f in FuzzMaskSQL FuzzGuardDDL FuzzExtractDeps; do $(GO) test ./internal/recovery/ -run '^$$' -fuzz "^$$f$$" -fuzztime 30s || exit 1; done

test-e2e: ## needs a running cluster and E2E_* variables: see test/e2e/README.md
	$(GO) test -tags e2e -count=1 -timeout 10m ./test/e2e/ -v

lint:
	golangci-lint run ./...

fmt:
	gofmt -w ./cmd ./internal ./test

tidy:
	$(GO) mod tidy

docker:
	docker build -t ddl-saver:dev .

clean:
	rm -rf bin
