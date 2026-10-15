.PHONY: build run test

build:
	go build -o bin/kvd ./cmd/kvd

run:
	go run ./cmd/kvd

test:
	go test -race ./...
