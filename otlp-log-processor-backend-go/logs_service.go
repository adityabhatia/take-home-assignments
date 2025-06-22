package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newServer(addr string, processor LogProcessor, logger *slog.Logger) collogspb.LogsServiceServer {
	return &dash0LogsServiceServer{
		addr:      addr,
		processor: processor,
		logger:    logger,
	}
}

// Export handles incoming log export requests, processes them, and returns a response with metrics.
func (s *dash0LogsServiceServer) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	if req == nil {
		atomic.AddInt64(&s.errors, 1)
		s.logger.WarnContext(ctx, "nil request received")

		//nolint:wrapcheck
		return nil, status.Error(codes.InvalidArgument, "request is nil")
	}

	s.logger.DebugContext(ctx, "received export request", "resource_logs", len(req.GetResourceLogs()))

	var records []LogRecord
	for _, rl := range req.GetResourceLogs() {
		records = append(records, s.extractRecordsFromResourceLogs(rl)...)
	}

	// Update metrics
	logsReceivedCounter.Add(ctx, 1)
	atomic.AddInt64(&s.logsReceivedCounter, int64(len(records)))
	atomic.AddInt64(&s.batchesReceivedCounter, 1)

	if len(records) > 0 {
		if err := s.processor.ProcessLogBatch(ctx, records); err != nil {
			atomic.AddInt64(&s.errors, 1)
			s.logger.ErrorContext(ctx, "failed to process batch", "error", err, "batch_size", len(records))

			return nil, fmt.Errorf("failed to process batch: %w", err)
		}
	}

	// Log the number of records processed
	s.logger.InfoContext(ctx, "processed export request", "records", len(records))

	return &collogspb.ExportLogsServiceResponse{}, nil
}
