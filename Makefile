.DEFAULT_GOAL := build

BUF_VERSION := 1.74.0
GOLANGCI_LINT_VERSION := 2.14.0
SEEDS ?= 10000

.PHONY: build run test sim tools fmt fmt-check lint proto proto-lint proto-check check

build:
	go build -o bin/kvd ./cmd/kvd

run:
	go run ./cmd/kvd

test:
	go test -race ./...

sim:
	go test -race -count=1 -timeout=20m ./test/sim -run '^TestSeededReplication$$' $(if $(filter undefined,$(origin SEED)),,-v) \
		-args -seeds "$(SEEDS)" $(if $(filter undefined,$(origin SEED)),,-seed "$(SEED)" -trace)

tools:
	@set -eu; \
	tools_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$tools_tmp"' EXIT; \
	buf_asset="buf-$$(uname -s)-$$(uname -m)"; \
	lint_dir="golangci-lint-$(GOLANGCI_LINT_VERSION)-$$(go env GOHOSTOS)-$$(go env GOHOSTARCH)"; \
	curl -fsSL --retry 3 --max-time 180 "https://github.com/bufbuild/buf/releases/download/v$(BUF_VERSION)/$$buf_asset" -o "$$tools_tmp/$$buf_asset"; \
	curl -fsSL --retry 3 --max-time 60 "https://github.com/bufbuild/buf/releases/download/v$(BUF_VERSION)/sha256.txt" -o "$$tools_tmp/buf-sha256.txt"; \
	(cd "$$tools_tmp" && sha256sum --check --ignore-missing buf-sha256.txt); \
	curl -fsSL --retry 3 --max-time 180 "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/$$lint_dir.tar.gz" -o "$$tools_tmp/$$lint_dir.tar.gz"; \
	curl -fsSL --retry 3 --max-time 60 "https://github.com/golangci/golangci-lint/releases/download/v$(GOLANGCI_LINT_VERSION)/golangci-lint-$(GOLANGCI_LINT_VERSION)-checksums.txt" -o "$$tools_tmp/lint-sha256.txt"; \
	(cd "$$tools_tmp" && sha256sum --check --ignore-missing lint-sha256.txt); \
	tar -xzf "$$tools_tmp/$$lint_dir.tar.gz" -C "$$tools_tmp" "$$lint_dir/golangci-lint"; \
	mkdir -p bin; \
	install -m 755 "$$tools_tmp/$$buf_asset" bin/buf; \
	install -m 755 "$$tools_tmp/$$lint_dir/golangci-lint" bin/golangci-lint
	bin/buf --version
	bin/golangci-lint version

fmt:
	gofmt -w .
	bin/buf format -w

fmt-check:
	@set -eu; files=$$(gofmt -l .); \
	if [ -n "$$files" ]; then printf 'Run make fmt to format:\n%s\n' "$$files"; exit 1; fi
	bin/buf format --diff --exit-code

lint:
	bin/golangci-lint run --no-config ./...

proto:
	bin/buf generate

proto-lint:
	bin/buf lint

proto-check:
	@set -eu; generated_tmp=$$(mktemp -d); \
	trap 'rm -rf "$$generated_tmp"' EXIT; \
	bin/buf generate --output "$$generated_tmp"; \
	diff -ru pkg/api "$$generated_tmp/pkg/api"

check: fmt-check proto-lint proto-check lint test build
