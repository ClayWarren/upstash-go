.PHONY: all build test lint fmt clean

all: build test lint

build:
	go build ./...

test:
	go test -race -covermode=atomic ./...

lint:
	golangci-lint run

fmt:
	go fmt ./...

clean:
	go clean
	rm -f coverage.txt
