# OTLP Log Parser (Go)

## Introduction
This take-home assignment is designed to give you an opportunity to demonstrate your skills and experience in
building a small backend application. We expect you to spend 3-4 hours on this assignment. If you find yourself spending more time
than that, please stop and submit what you have. We are not looking for a complete solution, but rather a demonstration
of your skills and experience.

To submit your solution, please create a public GitHub repository and send us the link. Please include a `README.md` file
with instructions on how to run your application.

## Overview
The goal of this assignment is to build a simple backend application that receives [log records](https://opentelemetry.io/docs/concepts/signals/logs/)
on a gRPC endpoint and processes them. Based on a **configurable attribute key and duration**, the application has to keep
counts of the number of unique log records per distinct attribute value. And within each window (configurable duration) print /
log these counts to stdout.
Note that the configurable attribute may appear either on Resource, Scope or Log level.

Pseudo example:
- "my log body 1" - {"foo":"bar", "baz":"qux"}
- "my log body 2" - {"foo":"qux", "baz":"qux"}
- "my log body 3" - {"baz":"qux"}
- "my log body 4" - {"foo":"baz"}
- "my log body 5" - {"foo":"baz", "baz":"qux"}

For example for configured attribute key "foo" it should report:
- "bar" - 1
- "qux" - 1
- "baz" - 2
- unknown - 1

Your solution should take into account high throughput, both in number of messages and the number of records per message.

Feel free to use the existing scaffoling in this folder, for example by fleshing out the implementation of the `Export`
method in `logs_service.go`. Of course, you can also change anything else as you see fit.

## Technology Constraints
- Your Go program should compile using standard Go SDK, and be compatible with Go 1.23.
- Use any additional libraries you want and need.

## Notes
- As this assignment is for the role of a Senior Product Engineer, we expect you to pay some attention to maintainability and operability of the solution. For example:
  - Consistent terminology usage
  - Validation of the behaviour
  - Include signals / events to help in debugging
- Assume that this application will be deployed to production. Build it accordingly.

## Usage

Build the application:
```shell
go build ./...
```

Run the application:
```shell
go run ./...
```

Run tests
```shell
go test ./...
```

## References

- [OpenTelemetry Logs](https://opentelemetry.io/docs/concepts/signals/logs/)
- [OpenTelemetry Protocol (OTLP)](https://github.com/open-telemetry/opentelemetry-proto)
- [OTLP Logs Examples](https://github.com/open-telemetry/opentelemetry-proto/blob/main/examples/logs.json)


# Solution

### Log Processor Backend
This application receives OpenTelemetry Protocol (OTLP) log records via a gRPC endpoint. It aggregates these logs by counting unique records based on a configurable attribute key and duration window, then outputs these counts to standard output.

Challenge Overview & Solution Highlights
The core challenge involved building a robust, observable, and high-throughput log processor.

### gRPC Endpoint for Log Reception:

The `dash0LogsServiceServer` struct implements the gRPC `LogsServiceServer` interface. Its `Export` method serves as the main entry point for all incoming OTLP log batches, allowing the application to receive logs efficiently.

- Command-line flags (`-key`, `-window`) enable users to dynamically set the desired aggregation attribute and the duration of each counting window. The `LogAggregator` internally uses a `time.Ticker` to precisely manage and rotate these aggregation `windows`. Log attributes are extracted with a clear priority: values found at the `Log` level override those at the `Scope` level, which in turn override those at the `Resource` level. If the target attribute is not present at any level, it defaults to `unknown` category.

- A custom `LogRecord` struct standardizes the internal representation of each log entry, specifically storing the extracted attribute value. The `AttributeCounter` struct, protected by a `sync.RWMutex` for thread safety, maintains a map to store the counts for each unique attribute value. Helper functions like `extractAttributes` and `getStringValue` reliably parse and convert various OTLP AnyValue types into consistent string representations for counting.

- The Log Aggregator's `runRotator` goroutine is responsible for triggering the `rotateWindow` function when the configured time window elapses. The `rotateWindow` function captures a snapshot of the current counts, logs summary information using structured logging (`slog`), and then prints a formatted, human-readable table of each attribute value and its corresponding count directly to stdout.

- Asynchronous processing is implemented using a buffered `processingChan` and a dedicated `runProcessor` goroutine. This design decouples the gRPC Export handler from the actual log processing logic, allowing the server to quickly acknowledge receipt of logs. A `sync.RWMutex` is used to ensure thread-safe access to shared data structures like the `AttributeCounter`. For graceful shutdowns, a `sync.WaitGroup` and `shutdownChan` are employed to ensure all pending log batches are fully processed before the application exits, minimizing data loss. The gRPC server is configured with `grpc.MaxRecvMsgSize` to accommodate large payloads, and if the processing queue reaches its capacity, it applies back pressure by returning a `ResourceExhausted` gRPC error to the client, preventing unbounded resource consumption.

- The project includes extensive unit, integration, and benchmark tests (`_test.go` files) to thoroughly validate behavior and performance characteristics.

- Structured Logging (`slog`): Utilizes Go's slog package for detailed, configurable, and machine-readable logs, crucial for debugging and operational insights in production environments.

- OpenTelemetry Metrics: Integrates with OpenTelemetry for tracking key application metrics (e.g., logs received, batches processed), providing essential observability for monitoring.

- Production Readiness: Features include robust graceful shutdown logic, comprehensive error handling, efficient concurrency management, and flexible command-line configuration.

- Miscellaneous improvements: Makefile with all utility commands, golangci-lint checks added to Github action workflows (configuration added to the root of the repository `.golangci.yml`), lint issues fixed

### Project Structure

```
./otlp-log-processor-backend-go
├── go.mod
├── go.sum
├── logs_export_integration_test.go
├── logs_processor.go
├── logs_processor_test.go
├── logs_service.go
├── otel.go
├── server.go
├── server_test.go
├── types.go
├── Dockerfile
└── Makefile
```

### Makefile Commands
The Makefile provides convenient commands for development and testing:

- `make init`: Initializes Go modules and downloads dependencies.
- `make lint`: Downloads and runs `golangci-lint` to check for lint issues.
- `make test`: Executes all unit and integration tests, generating a coverage report.
- `make build`: Compiles the Go application, creating the otlp-log-processor-backend-go executable.
- `make benchmark`: Runs performance benchmarks for the application.
- `make run`: Runs the application
- `make docker-build`: Builds a docker image called `otlp-log-processor`

### Prerequisites
- Go 1.23+
- Docker (optional)

### Run Locally
```
./otlp-log-processor-backend -key "my.custom.attribute" -window 60 -loglevel DEBUG
```

Observe the console for the aggregated counts appearing in your running application.