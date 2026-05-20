.PHONY: build test vet fmt tidy run clean

GO ?= go

build:
	$(GO) build ./...

test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

fmt:
	$(GO) fmt ./...

tidy:
	$(GO) mod tidy

run:
	$(GO) run ./cmd/example

clean:
	rm -rf bin dist coverage.txt
