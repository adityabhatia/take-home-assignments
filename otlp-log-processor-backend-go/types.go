package main

import (
	"context"
	"log/slog"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
)

const (
	MaxPendingRecords     = 100000
	UnknownAttributeValue = "unknown"
)

type LogRecord struct {
	AttributeValue string
	Timestamp      time.Time
	Body           string
}

type dash0LogsServiceServer struct {
	addr string
	collogspb.UnimplementedLogsServiceServer

	processor LogProcessor
	logger    *slog.Logger

	errors int64

	logsReceivedCounter    int64
	batchesReceivedCounter int64
}

type LogProcessor interface {
	ProcessLogBatch(ctx context.Context, records []LogRecord) error
	GetAttributeKey() string
	Shutdown()
}

type AttributeCounter struct {
	mu           sync.RWMutex
	counts       map[string]int64
	windowEnd    time.Time
	totalLogs    int64
	totalBatches int64
}

type logBatch struct {
	records []LogRecord
	ctx     context.Context
}

type LogAggregator struct {
	mu             sync.RWMutex
	config         *Config
	currentWindow  *AttributeCounter
	windowTicker   *time.Ticker
	processingChan chan *logBatch
	shutdownChan   chan struct{}
	wg             sync.WaitGroup
	logger         *slog.Logger
}
