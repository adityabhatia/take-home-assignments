package main

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Mock processor for integration testing
type mockIntegrationProcessor struct {
	mu             sync.RWMutex
	batches        [][]LogRecord
	attributeKey   string
	processError   error
	processDelay   time.Duration
	shutdownCalled bool
}

func newMockIntegrationProcessor(attributeKey string) *mockIntegrationProcessor {
	return &mockIntegrationProcessor{
		attributeKey: attributeKey,
		batches:      make([][]LogRecord, 0),
	}
}

func (m *mockIntegrationProcessor) ProcessLogBatch(ctx context.Context, records []LogRecord) error {
	if m.processDelay > 0 {
		time.Sleep(m.processDelay)
	}

	if m.processError != nil {
		return m.processError
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Make a copy of records to avoid race conditions
	recordsCopy := make([]LogRecord, len(records))
	copy(recordsCopy, records)
	m.batches = append(m.batches, recordsCopy)

	return nil
}

func (m *mockIntegrationProcessor) GetAttributeKey() string {
	return m.attributeKey
}

func (m *mockIntegrationProcessor) Shutdown() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.shutdownCalled = true
}

func (m *mockIntegrationProcessor) GetBatches() [][]LogRecord {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([][]LogRecord, len(m.batches))
	for i, batch := range m.batches {
		result[i] = make([]LogRecord, len(batch))
		copy(result[i], batch)
	}
	return result
}

func (m *mockIntegrationProcessor) GetTotalRecords() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	total := 0
	for _, batch := range m.batches {
		total += len(batch)
	}
	return total
}

func (m *mockIntegrationProcessor) SetProcessError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processError = err
}

func (m *mockIntegrationProcessor) SetProcessDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processDelay = delay
}

func (m *mockIntegrationProcessor) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.batches = make([][]LogRecord, 0)
	m.processError = nil
	m.processDelay = 0
}

// Test Export method integration tests
func TestExportIntegration(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	t.Run("successful export with single resource log", func(t *testing.T) {
		processor := newMockIntegrationProcessor("dash0.service")
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		request := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "dash0.service",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"},
								},
							},
							{
								Key: "service.version",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "1.0.0"},
								},
							},
						},
					},
					ScopeLogs: []*logspb.ScopeLogs{
						{
							Scope: &commonpb.InstrumentationScope{
								Name:    "test-logger",
								Version: "1.0.0",
							},
							LogRecords: []*logspb.LogRecord{
								{
									TimeUnixNano: uint64(time.Now().UnixNano()),
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Test log message 1"},
									},
								},
								{
									TimeUnixNano: uint64(time.Now().UnixNano()),
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Test log message 2"},
									},
									Attributes: []*commonpb.KeyValue{
										{
											Key: "log.level",
											Value: &commonpb.AnyValue{
												Value: &commonpb.AnyValue_StringValue{StringValue: "ERROR"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		}

		ctx := context.Background()
		response, err := server.Export(ctx, request)

		// Verify response
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if response == nil {
			t.Fatal("Expected response, got nil")
		}

		// Verify processor received the logs
		batches := processor.GetBatches()
		if len(batches) != 1 {
			t.Fatalf("Expected 1 batch, got %d", len(batches))
		}

		if len(batches[0]) != 2 {
			t.Fatalf("Expected 2 records in batch, got %d", len(batches[0]))
		}

		// Verify record content
		record1 := batches[0][0]
		if record1.AttributeValue != "test-service" {
			t.Errorf("Expected service name 'test-service', got '%s'", record1.AttributeValue)
		}
		if record1.Body != "Test log message 1" {
			t.Errorf("Expected body 'Test log message 1', got '%s'", record1.Body)
		}

		record2 := batches[0][1]
		if record2.AttributeValue != "test-service" {
			t.Errorf("Expected service name 'test-service', got '%s'", record2.AttributeValue)
		}
		if record2.Body != "Test log message 2" {
			t.Errorf("Expected body 'Test log message 2', got '%s'", record2.Body)
		}

		// Verify metrics
		if atomic.LoadInt64(&server.logsReceivedCounter) != 2 {
			t.Errorf("Expected logsReceivedCounter to be 2, got %d", atomic.LoadInt64(&server.logsReceivedCounter))
		}
		if atomic.LoadInt64(&server.batchesReceivedCounter) != 1 {
			t.Errorf("Expected batchesReceivedCounter to be 1, got %d", atomic.LoadInt64(&server.batchesReceivedCounter))
		}
	})

	t.Run("export with multiple resource logs", func(t *testing.T) {
		processor := newMockIntegrationProcessor("dash0.service")
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		request := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "dash0.service",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "service-1"},
								},
							},
						},
					},
					ScopeLogs: []*logspb.ScopeLogs{
						{
							LogRecords: []*logspb.LogRecord{
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Log from service 1"},
									},
								},
							},
						},
					},
				},
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "dash0.service",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "service-2"},
								},
							},
						},
					},
					ScopeLogs: []*logspb.ScopeLogs{
						{
							LogRecords: []*logspb.LogRecord{
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Log from service 2"},
									},
								},
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Another log from service 2"},
									},
								},
							},
						},
					},
				},
			},
		}

		ctx := context.Background()
		response, err := server.Export(ctx, request)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if response == nil {
			t.Fatal("Expected response, got nil")
		}

		// Verify all records were processed
		if processor.GetTotalRecords() != 3 {
			t.Fatalf("Expected 3 total records, got %d", processor.GetTotalRecords())
		}

		batches := processor.GetBatches()
		if len(batches) != 1 {
			t.Fatalf("Expected 1 batch, got %d", len(batches))
		}

		records := batches[0]
		if len(records) != 3 {
			t.Fatalf("Expected 3 records, got %d", len(records))
		}

		// Verify records from different services
		serviceNames := make(map[string]int)
		for _, record := range records {
			serviceNames[record.AttributeValue]++
		}

		if serviceNames["service-1"] != 1 {
			t.Errorf("Expected 1 record from service-1, got %d", serviceNames["service-1"])
		}
		if serviceNames["service-2"] != 2 {
			t.Errorf("Expected 2 records from service-2, got %d", serviceNames["service-2"])
		}
	})

	t.Run("export with attribute priority resolution", func(t *testing.T) {
		processor := newMockIntegrationProcessor("dash0.service")
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		request := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "dash0.service",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "resource-service"},
								},
							},
						},
					},
					ScopeLogs: []*logspb.ScopeLogs{
						{
							Scope: &commonpb.InstrumentationScope{
								Attributes: []*commonpb.KeyValue{
									{
										Key: "dash0.service",
										Value: &commonpb.AnyValue{
											Value: &commonpb.AnyValue_StringValue{StringValue: "scope-service"},
										},
									},
								},
							},
							LogRecords: []*logspb.LogRecord{
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Uses scope service name"},
									},
								},
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Uses log service name"},
									},
									Attributes: []*commonpb.KeyValue{
										{
											Key: "dash0.service",
											Value: &commonpb.AnyValue{
												Value: &commonpb.AnyValue_StringValue{StringValue: "log-service"},
											},
										},
									},
								},
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Falls back to resource"},
									},
									Attributes: []*commonpb.KeyValue{
										{
											Key: "other.attr",
											Value: &commonpb.AnyValue{
												Value: &commonpb.AnyValue_StringValue{StringValue: "value"},
											},
										},
									},
								},
							},
						},
						{
							// Scope without dash0.service attribute
							LogRecords: []*logspb.LogRecord{
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Uses resource service name"},
									},
								},
							},
						},
					},
				},
			},
		}

		ctx := context.Background()
		response, err := server.Export(ctx, request)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if response == nil {
			t.Fatal("Expected response, got nil")
		}

		batches := processor.GetBatches()
		if len(batches) != 1 {
			t.Fatalf("Expected 1 batch, got %d", len(batches))
		}

		records := batches[0]
		if len(records) != 4 {
			t.Fatalf("Expected 4 records, got %d", len(records))
		}

		// Verify attribute priority resolution
		expectedServices := []string{"scope-service", "log-service", "scope-service", "resource-service"}
		for i, expected := range expectedServices {
			if records[i].AttributeValue != expected {
				t.Logf("%v", records)
				t.Errorf("Record %d: expected service name '%s', got '%s'", i, expected, records[i].AttributeValue)
			}
		}
	})

	t.Run("export with missing attribute falls back to unknown", func(t *testing.T) {
		processor := newMockIntegrationProcessor("missing.attribute")
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		request := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{
				{
					Resource: &resourcepb.Resource{
						Attributes: []*commonpb.KeyValue{
							{
								Key: "dash0.service",
								Value: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "test-service"},
								},
							},
						},
					},
					ScopeLogs: []*logspb.ScopeLogs{
						{
							LogRecords: []*logspb.LogRecord{
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Test log"},
									},
								},
							},
						},
					},
				},
			},
		}

		ctx := context.Background()
		response, err := server.Export(ctx, request)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if response == nil {
			t.Fatal("Expected response, got nil")
		}

		batches := processor.GetBatches()
		records := batches[0]

		if records[0].AttributeValue != UnknownAttributeValue {
			t.Errorf("Expected unknown attribute value '%s', got '%s'", UnknownAttributeValue, records[0].AttributeValue)
		}
	})

	t.Run("export with nil request", func(t *testing.T) {
		processor := newMockIntegrationProcessor("dash0.service")
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		ctx := context.Background()
		response, err := server.Export(ctx, nil)

		if response != nil {
			t.Error("Expected nil response for nil request")
		}

		if err == nil {
			t.Fatal("Expected error for nil request")
		}

		if status.Code(err) != codes.InvalidArgument {
			t.Errorf("Expected InvalidArgument error, got %v", status.Code(err))
		}
	})

	t.Run("export with empty request", func(t *testing.T) {
		processor := newMockIntegrationProcessor("dash0.service")
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		request := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{},
		}

		ctx := context.Background()
		response, err := server.Export(ctx, request)

		if err != nil {
			t.Fatalf("Expected no error for empty request, got %v", err)
		}

		if response == nil {
			t.Fatal("Expected response, got nil")
		}

		// Verify no batches were processed
		if processor.GetTotalRecords() != 0 {
			t.Errorf("Expected 0 records processed, got %d", processor.GetTotalRecords())
		}

		// Verify metrics
		if atomic.LoadInt64(&server.batchesReceivedCounter) != 1 {
			t.Errorf("Expected batchesReceivedCounter to be 1, got %d", atomic.LoadInt64(&server.batchesReceivedCounter))
		}
		if atomic.LoadInt64(&server.logsReceivedCounter) != 0 {
			t.Errorf("Expected logsReceivedCounter to be 0, got %d", atomic.LoadInt64(&server.logsReceivedCounter))
		}
	})

	t.Run("export with processor error", func(t *testing.T) {
		processor := newMockIntegrationProcessor("dash0.service")
		processor.SetProcessError(status.Error(codes.Internal, "processing failed"))
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		request := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{
				{
					ScopeLogs: []*logspb.ScopeLogs{
						{
							LogRecords: []*logspb.LogRecord{
								{
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "Test log"},
									},
								},
							},
						},
					},
				},
			},
		}

		ctx := context.Background()
		response, err := server.Export(ctx, request)

		if response != nil {
			t.Error("Expected nil response when processor fails")
		}

		if err == nil {
			t.Fatal("Expected error when processor fails")
		}

		if status.Code(err) != codes.Internal {
			t.Errorf("Expected Internal error, got %v", status.Code(err))
		}
	})

	t.Run("export with various data types", func(t *testing.T) {
		processor := newMockIntegrationProcessor("test.attribute")
		server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

		request := &collogspb.ExportLogsServiceRequest{
			ResourceLogs: []*logspb.ResourceLogs{
				{
					ScopeLogs: []*logspb.ScopeLogs{
						{
							LogRecords: []*logspb.LogRecord{
								{
									Attributes: []*commonpb.KeyValue{
										{
											Key: "test.attribute",
											Value: &commonpb.AnyValue{
												Value: &commonpb.AnyValue_StringValue{StringValue: "string-value"},
											},
										},
									},
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_StringValue{StringValue: "String log"},
									},
								},
								{
									Attributes: []*commonpb.KeyValue{
										{
											Key: "test.attribute",
											Value: &commonpb.AnyValue{
												Value: &commonpb.AnyValue_IntValue{IntValue: 42},
											},
										},
									},
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_IntValue{IntValue: 123},
									},
								},
								{
									Attributes: []*commonpb.KeyValue{
										{
											Key: "test.attribute",
											Value: &commonpb.AnyValue{
												Value: &commonpb.AnyValue_BoolValue{BoolValue: true},
											},
										},
									},
									Body: &commonpb.AnyValue{
										Value: &commonpb.AnyValue_DoubleValue{DoubleValue: 3.14},
									},
								},
							},
						},
					},
				},
			},
		}

		ctx := context.Background()
		response, err := server.Export(ctx, request)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if response == nil {
			t.Fatal("Expected response, got nil")
		}

		batches := processor.GetBatches()
		records := batches[0]

		if len(records) != 3 {
			t.Fatalf("Expected 3 records, got %d", len(records))
		}

		// Verify different data types are handled correctly
		expectedAttributes := []string{"string-value", "42", "true"}
		expectedBodies := []string{"String log", "123", "3.14"}

		for i, record := range records {
			if record.AttributeValue != expectedAttributes[i] {
				t.Errorf("Record %d: expected attribute '%s', got '%s'", i, expectedAttributes[i], record.AttributeValue)
			}
			if record.Body != expectedBodies[i] {
				t.Errorf("Record %d: expected body '%s', got '%s'", i, expectedBodies[i], record.Body)
			}
		}
	})
}

// Benchmark tests for Export method
func BenchmarkExport(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	processor := newMockIntegrationProcessor("dash0.service")
	server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "dash0.service",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{StringValue: "benchmark-service"},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: []*logspb.LogRecord{
							{
								Body: &commonpb.AnyValue{
									Value: &commonpb.AnyValue_StringValue{StringValue: "Benchmark log message"},
								},
							},
						},
					},
				},
			},
		},
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor.Reset()
		_, err := server.Export(ctx, request)
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
}

func BenchmarkExportLarge(b *testing.B) {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))
	processor := newMockIntegrationProcessor("dash0.service")
	server := newServer("localhost:4317", processor, logger).(*dash0LogsServiceServer)

	// Create a large request with many log records
	logRecords := make([]*logspb.LogRecord, 1000)
	for i := 0; i < 1000; i++ {
		logRecords[i] = &logspb.LogRecord{
			Body: &commonpb.AnyValue{
				Value: &commonpb.AnyValue_StringValue{StringValue: "Benchmark log message"},
			},
		}
	}

	request := &collogspb.ExportLogsServiceRequest{
		ResourceLogs: []*logspb.ResourceLogs{
			{
				Resource: &resourcepb.Resource{
					Attributes: []*commonpb.KeyValue{
						{
							Key: "dash0.service",
							Value: &commonpb.AnyValue{
								Value: &commonpb.AnyValue_StringValue{StringValue: "benchmark-service"},
							},
						},
					},
				},
				ScopeLogs: []*logspb.ScopeLogs{
					{
						LogRecords: logRecords,
					},
				},
			},
		},
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		processor.Reset()
		_, err := server.Export(ctx, request)
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
	}
}
