package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// Global logger for tests. Just basic info level.
var testLogger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))

// Simple mock for the processor interface, so tests don't need real implementation.
type mockProcessor struct {
	attributeKey string // Stores the key it's configured with
}

// ProcessLogBatch is a no-op for the mock.
func (m *mockProcessor) ProcessLogBatch(ctx context.Context, records []LogRecord) error {
	return nil
}

// GetAttributeKey just returns what was set.
func (m *mockProcessor) GetAttributeKey() string {
	return m.attributeKey
}

// Shutdown does nothing for the mock.
func (m *mockProcessor) Shutdown() {
	// Nothing to clean up here.
}

func TestAttributeCounter(t *testing.T) {
	t.Run("NewAttributeCounter", func(t *testing.T) {
		someFutureTime := time.Now().Add(time.Hour)
		counter := NewAttributeCounter(someFutureTime)

		if counter == nil {
			t.Fatal("NewAttributeCounter returned a nil object, expected a valid counter.")
		}

		// Check initial state
		if counter.windowEnd != someFutureTime {
			t.Errorf("Window end mismatch. Expected %v, got %v", someFutureTime, counter.windowEnd)
		}

		if counter.counts == nil {
			t.Error("Counts map was unexpectedly nil after creation.")
		}

		if counter.totalLogs != 0 {
			t.Errorf("Total logs should start at 0, but was %d", counter.totalLogs)
		}

		if counter.totalBatches != 0 {
			t.Errorf("Total batches should start at 0, but was %d", counter.totalBatches)
		}
	})

	t.Run("Add", func(t *testing.T) {
		counter := NewAttributeCounter(time.Now()) // Window end doesn't really matter for 'Add' test

		counter.Add("serviceA", 5)
		counter.Add("serviceB", 3)
		counter.Add("serviceA", 2) // Adding more to serviceA

		counts, totalLogs, totalBatches := counter.Snapshot()

		// Verify counts for individual services
		if counts["serviceA"] != 7 {
			t.Errorf("ServiceA count incorrect. Expected 7, got %d", counts["serviceA"])
		}

		if counts["serviceB"] != 3 {
			t.Errorf("ServiceB count incorrect. Expected 3, got %d", counts["serviceB"])
		}

		// Verify overall totals
		if totalLogs != 10 {
			t.Errorf("Total logs expected 10, got %d", totalLogs)
		}

		if totalBatches != 3 { // 3 separate Add calls
			t.Errorf("Total batches expected 3, got %d", totalBatches)
		}
	})

	t.Run("Snapshot thread safety", func(t *testing.T) {
		counter := NewAttributeCounter(time.Now())

		var wg sync.WaitGroup

		const (
			writers            = 10
			addsPerWriter      = 100
			readers            = 5
			snapshotsPerReader = 50
		)

		// Goroutines that add data concurrently
		for range writers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range addsPerWriter {
					counter.Add("threaded_service", 1)
				}
			}()
		}

		// Goroutines that read snapshots concurrently
		for range readers {
			wg.Add(1)

			go func() {
				defer wg.Done()

				for range snapshotsPerReader {
					// Just call snapshot, we don't need to check values in concurrent reads
					counter.Snapshot()
				}
			}()
		}

		wg.Wait() // Wait for all goroutines to finish

		finalCounts, finalTotalLogs, finalTotalBatches := counter.Snapshot()

		expectedLogs := writers * addsPerWriter
		expectedBatches := writers * addsPerWriter // Each 'Add' call counts as a batch

		if finalTotalLogs != int64(expectedLogs) {
			t.Errorf("Thread safety failed for total logs. Expected %d, got %d", expectedLogs, finalTotalLogs)
		}

		if finalTotalBatches != int64(expectedBatches) {
			t.Errorf("Thread safety failed for total batches. Expected %d, got %d", expectedBatches, finalTotalBatches)
		}

		if finalCounts["threaded_service"] != int64(expectedLogs) {
			t.Errorf("Thread safety failed for service count. Expected %d, got %d", expectedLogs, finalCounts["threaded_service"])
		}
	})
}

func TestLogAggregator(t *testing.T) {
	// A dedicated logger for aggregator tests, maybe debug level
	aggregatorLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	t.Run("NewLogProcessor", func(t *testing.T) {
		cfg := &Config{
			AttributeKey:   "host.name", // Using a different key here just for variety
			WindowDuration: 5 * time.Second,
		}
		processor := NewLogProcessor(cfg, aggregatorLogger)

		if processor == nil {
			t.Fatalf("NewLogProcessor returned nil, it shouldn't.")
		}

		if processor.GetAttributeKey() != cfg.AttributeKey {
			t.Errorf("Processor did not correctly set attribute key. Expected '%s', got '%s'", cfg.AttributeKey, processor.GetAttributeKey())
		}

		processor.Shutdown() // Important to clean up resources
	})

	t.Run("ProcessLogBatch empty batch", func(t *testing.T) {
		cfg := &Config{AttributeKey: "service.dash0", WindowDuration: time.Second}

		processor := NewLogProcessor(cfg, aggregatorLogger)
		defer processor.Shutdown()

		ctx := context.Background()

		err := processor.ProcessLogBatch(ctx, []LogRecord{}) // Empty slice
		if err != nil {
			t.Errorf("Processing an empty batch should not produce an error, but got: %v", err)
		}
	})

	t.Run("ProcessLogBatch with records", func(t *testing.T) {
		cfg := &Config{AttributeKey: "service.dash0", WindowDuration: time.Second}

		processor := NewLogProcessor(cfg, aggregatorLogger)
		defer processor.Shutdown()

		sampleRecords := []LogRecord{
			{AttributeValue: "app-server", Timestamp: time.Now(), Body: "User logged in."},
			{AttributeValue: "db-server", Timestamp: time.Now(), Body: "Query failed."},
			{AttributeValue: "app-server", Timestamp: time.Now(), Body: "Page loaded."},
		}

		ctx := context.Background()

		err := processor.ProcessLogBatch(ctx, sampleRecords)
		if err != nil {
			t.Errorf("Error processing valid log batch: %v", err)
		}

		// Give it a moment to process async, otherwise tests might finish too fast
		time.Sleep(100 * time.Millisecond)
	})
}

func TestUtilityFunctions(t *testing.T) {
	t.Run("getStringValue", func(t *testing.T) {
		tests := []struct {
			name     string
			input    *commonpb.AnyValue
			expected string
		}{
			{"nil input", nil, ""},
			{"string type", &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello_world"}}, "hello_world"},
			{"int type", &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 12345}}, "12345"},
			{"double type", &commonpb.AnyValue{Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 98.76}}, "98.76"},
			{"bool true", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}, "true"},
			{"bool false", &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: false}}, "false"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				actual := getStringValue(tc.input)
				if actual != tc.expected {
					t.Errorf("For input %v, expected %q but got %q", tc.input, tc.expected, actual)
				}
			})
		}
	})

	t.Run("extractLogBody", func(t *testing.T) {
		tests := []struct {
			name      string
			logRecord *logspb.LogRecord
			expected  string
		}{
			{
				"log with string body",
				&logspb.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "This is a log message."}}},
				"This is a log message.",
			},
			{
				"log with nil body",
				&logspb.LogRecord{Body: nil},
				"",
			},
			{
				"log with empty string body",
				&logspb.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: ""}}},
				"",
			},
			{
				"log with int body",
				&logspb.LogRecord{Body: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 789}}},
				"789",
			},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				actualBody := extractLogBody(tc.logRecord)
				if actualBody != tc.expected {
					t.Errorf("Body extraction failed. Expected %q, got %q", tc.expected, actualBody)
				}
			})
		}
	})
}

func TestFindAttributeValue(t *testing.T) {
	// Setup a server instance for the method call
	svr := &dash0LogsServiceServer{
		processor: &mockProcessor{attributeKey: "my.service.dash0"}, // The attribute key we're looking for
	}

	tests := []struct {
		name                                string
		logAttrs, scopeAttrs, resourceAttrs map[string]string
		expected                            string
	}{
		{
			"Log level has highest priority",
			map[string]string{"my.service.dash0": "from-log"},
			map[string]string{"my.service.dash0": "from-scope"},
			map[string]string{"my.service.dash0": "from-resource"},
			"from-log",
		},
		{
			"Scope level is fallback if log doesn't have it",
			map[string]string{"other.key": "xyz"},
			map[string]string{"my.service.dash0": "from-scope"},
			map[string]string{"my.service.dash0": "from-resource"},
			"from-scope",
		},
		{
			"Resource level is fallback if neither log nor scope have it",
			map[string]string{"k1": "v1"},
			map[string]string{"k2": "v2"},
			map[string]string{"my.service.dash0": "from-resource"},
			"from-resource",
		},
		{
			"Returns UnknownAttributeValue if not found anywhere",
			map[string]string{"foo": "bar"},
			map[string]string{"baz": "qux"},
			map[string]string{"abc": "123"},
			UnknownAttributeValue,
		},
		{
			"All maps nil",
			nil, nil, nil,
			UnknownAttributeValue,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := svr.findAttributeValue(tc.logAttrs, tc.scopeAttrs, tc.resourceAttrs)
			if result != tc.expected {
				t.Errorf("Failed for test '%s'. Expected '%s', got '%s'", tc.name, tc.expected, result)
			}
		})
	}
}

func TestExtractRecordsFromResourceLogs(t *testing.T) {
	server := &dash0LogsServiceServer{
		processor: &mockProcessor{attributeKey: "service.dash0"},
	}

	resourceLogs := &logspb.ResourceLogs{
		Resource: &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{
					Key: "service.dash0",
					Value: &commonpb.AnyValue{
						Value: &commonpb.AnyValue_StringValue{StringValue: "my-service"},
					},
				},
			},
		},
		ScopeLogs: []*logspb.ScopeLogs{
			{
				Scope: &commonpb.InstrumentationScope{
					Name: "test-scope",
				},
				LogRecords: []*logspb.LogRecord{
					{
						TimeUnixNano: uint64(time.Now().UnixNano()),
						Body: &commonpb.AnyValue{
							Value: &commonpb.AnyValue_StringValue{StringValue: "test message 1"},
						},
					},
					{
						TimeUnixNano: uint64(time.Now().UnixNano()),
						Body: &commonpb.AnyValue{
							Value: &commonpb.AnyValue_StringValue{StringValue: "test message 2"},
						},
						Attributes: []*commonpb.KeyValue{
							{
								Key: "service.dash0",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "override-service"},
								},
							},
						},
					},
				},
			},
		},
	}

	records := server.extractRecordsFromResourceLogs(resourceLogs)

	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}

	// First record should use resource-level service name
	if records[0].AttributeValue != "my-service" {
		t.Errorf("Expected first record service name 'my-service', got '%s'", records[0].AttributeValue)
	}

	if records[0].Body != "test message 1" {
		t.Errorf("Expected first record body 'test message 1', got '%s'", records[0].Body)
	}

	// Second record should use log-level service name (override)
	if records[1].AttributeValue != "override-service" {
		t.Errorf("Expected second record service name 'override-service', got '%s'", records[1].AttributeValue)
	}

	if records[1].Body != "test message 2" {
		t.Errorf("Expected second record body 'test message 2', got '%s'", records[1].Body)
	}
}

func TestExtractAttributes(t *testing.T) {
	t.Run("nil input", func(t *testing.T) {
		extracted := extractAttributes(nil)
		if len(extracted) != 0 {
			t.Errorf("Expected empty map for nil input, got %v", extracted)
		}
	})

	t.Run("Resourcepb Resource with attributes", func(t *testing.T) {
		res := &resourcepb.Resource{
			Attributes: []*commonpb.KeyValue{
				{Key: "service.dash0", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "my-app"}}},
				{Key: "os.type", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "linux"}}},
				{Key: "cpu.cores", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 8}}},
				{Key: "empty.value", Value: nil}, // Should be skipped
			},
		}
		attrs := extractAttributes(res)

		if len(attrs) != 3 {
			t.Fatalf("Expected 3 attributes, got %d: %v", len(attrs), attrs)
		}

		if attrs["service.dash0"] != "my-app" {
			t.Errorf("service.dash0 mismatch: expected 'my-app', got '%s'", attrs["service.dash0"])
		}

		if attrs["os.type"] != "linux" {
			t.Errorf("os.type mismatch: expected 'linux', got '%s'", attrs["os.type"])
		}

		if attrs["cpu.cores"] != "8" {
			t.Errorf("cpu.cores mismatch: expected '8', got '%s'", attrs["cpu.cores"])
		}

		if _, exists := attrs["empty.value"]; exists {
			t.Errorf("empty.value should not exist in map but it does: %s", attrs["empty.value"])
		}
	})

	t.Run("Commonpb InstrumentationScope with attributes", func(t *testing.T) {
		scope := &commonpb.InstrumentationScope{
			Name:    "my-instrumentation",
			Version: "1.2.3",
			Attributes: []*commonpb.KeyValue{
				{Key: "scope.key.str", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "scope_val_str"}}},
				{Key: "scope.key.bool", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: true}}},
			},
		}
		attrs := extractAttributes(scope)

		if len(attrs) != 2 {
			t.Fatalf("Expected 2 attributes for scope, got %d: %v", len(attrs), attrs)
		}

		if attrs["scope.key.str"] != "scope_val_str" {
			t.Errorf("scope.key.str mismatch: expected 'scope_val_str', got '%s'", attrs["scope.key.str"])
		}

		if attrs["scope.key.bool"] != "true" {
			t.Errorf("scope.key.bool mismatch: expected 'true', got '%s'", attrs["scope.key.bool"])
		}
	})

	t.Run("Logspb LogRecord with attributes", func(t *testing.T) {
		logRec := &logspb.LogRecord{
			Attributes: []*commonpb.KeyValue{
				{Key: "log.id", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: 1001}}},
				{Key: "log.status", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "success"}}},
			},
		}
		attrs := extractAttributes(logRec)

		if len(attrs) != 2 {
			t.Fatalf("Expected 2 attributes for log record, got %d: %v", len(attrs), attrs)
		}

		if attrs["log.id"] != "1001" {
			t.Errorf("log.id mismatch: expected '1001', got '%s'", attrs["log.id"])
		}

		if attrs["log.status"] != "success" {
			t.Errorf("log.status mismatch: expected 'success', got '%s'", attrs["log.status"])
		}
	})

	t.Run("unsupported type input", func(t *testing.T) {
		// A struct that doesn't implement the attribute interface
		unsupportedInput := struct{ SomeField string }{SomeField: "test"}
		attrs := extractAttributes(unsupportedInput)

		if len(attrs) != 0 {
			t.Errorf("Expected an empty map for unsupported type, but got %v", attrs)
		}
	})
}

func BenchmarkAttributeCounter_Add(b *testing.B) {
	counter := NewAttributeCounter(time.Now())

	b.ResetTimer() // Reset timer before the actual benchmark loop

	for range b.N {
		counter.Add("benchmark_service", 1) // Add a single count
	}
}

func BenchmarkAttributeCounter_Snapshot(b *testing.B) {
	counter := NewAttributeCounter(time.Now())

	// Pre-populate with some data so Snapshot actually does something
	for range 1000 {
		counter.Add("service_A", 1)
		counter.Add("service_B", 1)
	}

	b.ResetTimer()

	for range b.N {
		counter.Snapshot()
	}
}

func BenchmarkLogAggregator_ProcessLogBatch(b *testing.B) {
	// Use a discarded logger to prevent benchmark output noise
	benchLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	cfg := &Config{
		AttributeKey:   "service.dash0",
		WindowDuration: time.Hour, // Keep a long window to avoid rotation overhead
	}

	processor := NewLogProcessor(cfg, benchLogger)
	defer processor.Shutdown() // Ensure the processor is shut down after benchmark

	// Prepare a typical batch of records
	sampleBatch := []LogRecord{
		{AttributeValue: "bench_svc1", Timestamp: time.Now(), Body: "Log message one."},
		{AttributeValue: "bench_svc2", Timestamp: time.Now(), Body: "Log message two."},
		{AttributeValue: "bench_svc1", Timestamp: time.Now(), Body: "Log message three."},
	}

	ctx := context.Background()

	b.ResetTimer() // Start timing

	for range b.N {
		processor.ProcessLogBatch(ctx, sampleBatch)
	}
}
