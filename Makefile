.PHONY: build run test proto proto-lint

build:
	go build -o bin/kvd ./cmd/kvd

run:
	go run ./cmd/kvd

test:
	go test -race ./...

proto:
	buf generate

proto-lint:
	buf lint
