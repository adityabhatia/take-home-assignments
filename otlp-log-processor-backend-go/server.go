package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	listenAddr            = flag.String("listenAddr", "localhost:4317", "The listen address")
	maxReceiveMessageSize = flag.Int("maxReceiveMessageSize", 16777216, "The max message size in bytes the server can receive") //nolint:mnd
	attributeKey          = flag.String("key", "foo", "The attribute key to track for unique log counts (e.g., 'foo')")
	logLevel              = flag.String("loglevel", "INFO", "The log level to use (DEBUG, INFO, WARN, ERROR)")
	windowDuration        = flag.Int("window", 30, "The duration in seconds for each counting window") //nolint:mnd
)

const name = "dash0.com/otlp-log-processor-backend"

var (
	meter               = otel.Meter(name)
	logsReceivedCounter metric.Int64Counter
)

// Config holds the configuration for the log processor.
type Config struct {
	AttributeKey   string
	WindowDuration time.Duration
}

func init() {
	var err error

	logsReceivedCounter, err = meter.Int64Counter("com.dash0.homeexercise.logs.received",
		metric.WithDescription("The number of logs received by otlp-log-processor-backend"),
		metric.WithUnit("{log}"))
	if err != nil {
		panic(err)
	}
}

func main() {
	// Setup logger
	var level slog.Level

	switch *logLevel {
	case "DEBUG":
		level = slog.LevelDebug
	case "INFO":
		level = slog.LevelInfo
	case "WARN":
		level = slog.LevelWarn
	case "ERROR":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	}))

	if err := run(logger); err != nil {
		logger.Error("Failed to run otlp-log-processor-backend", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) (err error) {
	logger.Info("Starting dash0 otlp-log-processor-backend")

	config := &Config{
		AttributeKey:   *attributeKey,
		WindowDuration: time.Duration(*windowDuration) * time.Second,
	}

	// Create log processor
	processor := NewLogProcessor(config, logger)

	// Set up OpenTelemetry.
	otelShutdown, err := setupOTelSDK(context.Background())
	if err != nil {
		return err
	}

	// Handle shutdown properly so nothing leaks.
	defer func() {
		err = errors.Join(err, otelShutdown(context.Background()))
	}()

	flag.Parse()

	slog.Debug("Starting listener", slog.String("listenAddr", *listenAddr))

	listener, err := net.Listen("tcp", *listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", *listenAddr, err)
	}

	grpcServer := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(*maxReceiveMessageSize),
		grpc.Creds(insecure.NewCredentials()),
	)
	collogspb.RegisterLogsServiceServer(grpcServer, newServer(*listenAddr, NewLogProcessor(&Config{
		AttributeKey:   *attributeKey,
		WindowDuration: time.Duration(*windowDuration) * time.Second,
	}, logger), logger))

	logger.Info("Starting log aggregation service",
		"addr", listenAddr,
		"attribute_key", attributeKey,
		"window_duration", windowDuration,
		"log_level", logLevel)

	// Start server in goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			logger.Error("Failed to serve", "error", err)
			// Exit with non-zero status to indicate failure
			os.Exit(1)
		}
	}()

	// Wait for shutdown signal
	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, syscall.SIGINT, syscall.SIGTERM)
	<-shutdownChan

	logger.Info("Shutting down gracefully...")

	// Graceful shutdown
	grpcServer.GracefulStop()
	processor.Shutdown()

	logger.Info("Server stopped")

	//nolint:wrapcheck
	return err
}
