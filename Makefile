VERSION ?= 1.2.0
PKG      = chsync/internal/app
LDFLAGS  = -s -w -X $(PKG).version=$(VERSION)
GOFLAGS += -mod=vendor -trimpath
export GOFLAGS

.PHONY: build test lint fix dist clean

build:
	CGO_ENABLED=0 go build -ldflags "$(LDFLAGS)" -o chsync ./cmd/chsync

test:
	go vet ./...
	go test -race ./...

# Модернизаторы go fix (Go 1.26+): показать, что можно упростить под текущую версию языка.
fix:
	go fix -diff ./...

lint: test
	@test -z "$$(gofmt -l cmd internal)" || (gofmt -l cmd internal; exit 1)
	@command -v staticcheck >/dev/null && staticcheck ./... || echo "staticcheck не установлен — пропущено"

dist: lint
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/chsync-linux-amd64 ./cmd/chsync
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/chsync-linux-arm64 ./cmd/chsync
	cd dist && sha256sum chsync-linux-* > SHA256SUMS

clean:
	rm -rf chsync dist
