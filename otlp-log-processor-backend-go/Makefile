GOLANGCI_LINT_VERSION = v1.63.4
GOLANGCI_LINT = ./artifacts/bin/golangci-lint

.PHONY: golangci-lint-install lint test build test-benchmark init

init:
	@echo "Initializing Go modules and downloading dependencies..."
	go mod tidy
	go mod download
	@echo "Initialization complete."

golangci-lint-install:
	@if [ ! -f $(GOLANGCI_LINT) ]; then \
		echo "Installing golangci-lint..."; \
		mkdir -p ./artifacts/bin; \
		curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b ./artifacts/bin $(GOLANGCI_LINT_VERSION); \
	fi

lint: golangci-lint-install
	GOTOOLCHAIN=go1.23.4 $(GOLANGCI_LINT) run --timeout 5m

test:
	go test -v ./... -coverprofile=coverage.out

build:
	go build -o otlp-log-processor-backend

benchmark:
	go test -v ./... -bench=. -coverprofile=coverage.out

run:
	go run ./...

docker-build:
	docker build -t otlp-log-processor:latest . 