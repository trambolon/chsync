VERSION ?= 1.1.0
LDFLAGS  = -s -w -X main.version=$(VERSION)

.PHONY: build test dist clean

build:
	CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags "$(LDFLAGS)" -o chsync .

test:
	go vet -mod=vendor ./...
	go test -mod=vendor ./...

dist: test
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -mod=vendor -trimpath -ldflags "$(LDFLAGS)" -o dist/chsync-linux-amd64 .
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -mod=vendor -trimpath -ldflags "$(LDFLAGS)" -o dist/chsync-linux-arm64 .
	cd dist && sha256sum chsync-linux-* > SHA256SUMS

clean:
	rm -rf chsync dist
