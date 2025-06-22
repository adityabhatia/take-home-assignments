package main

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// NewAttributeCounter creates a new AttributeCounter for a given end time.
func NewAttributeCounter(end time.Time) *AttributeCounter {
	return &AttributeCounter{
		counts:    make(map[string]int64),
		windowEnd: end,
	}
}

// Add increments the count for a given attribute value and updates total logs and batches.
func (ac *AttributeCounter) Add(value string, count int64) {
	ac.mu.Lock()
	defer ac.mu.Unlock()
	ac.counts[value] += count
	ac.totalLogs += count
	ac.totalBatches++
}

// Snapshot returns a copy of the current counts, total logs, and total batches.
func (ac *AttributeCounter) Snapshot() (map[string]int64, int64, int64) {
	ac.mu.RLock()
	defer ac.mu.RUnlock()

	countCopy := make(map[string]int64, len(ac.counts))
	for k, v := range ac.counts {
		countCopy[k] = v
	}

	return countCopy, ac.totalLogs, ac.totalBatches
}

// NewLogProcessor creates a new LogProcessor instance
// It initializes the log aggregator with a ticker for rotating windows and a processing channel.
func NewLogProcessor(config *Config, logger *slog.Logger) LogProcessor {
	logAggregator := &LogAggregator{
		config:         config,
		currentWindow:  NewAttributeCounter(time.Now().Add(config.WindowDuration)),
		windowTicker:   time.NewTicker(config.WindowDuration),
		processingChan: make(chan *logBatch, 100), //nolint:mnd
		shutdownChan:   make(chan struct{}),
		logger:         logger,
	}
	logAggregator.wg.Add(2) //nolint:mnd

	// Start the processor and rotator in separate goroutines
	go logAggregator.runProcessor()
	go logAggregator.runRotator()

	return logAggregator
}

// ProcessLogBatch processes a batch and adds the batch to the processing channel if there's space, otherwise logs a warning and returns an error.
func (la *LogAggregator) ProcessLogBatch(ctx context.Context, records []LogRecord) error {
	if len(records) == 0 {
		return nil
	}
	select {
	case la.processingChan <- &logBatch{records, ctx}:
		la.logger.Info("Batch added to processing queue", "queue_length", len(la.processingChan))

		return nil
	default:
		la.logger.WarnContext(ctx, "Processing queue full, dropping batch", "batch_size", len(records), "queue_length", len(la.processingChan))

		//nolint:wrapcheck
		return status.Error(codes.ResourceExhausted, "processing queue full")
	}
}

// GetAttributeKey returns the attribute key used for aggregation.
func (la *LogAggregator) GetAttributeKey() string {
	return la.config.AttributeKey
}

func (la *LogAggregator) runProcessor() {
	defer la.wg.Done()

	for {
		select {
		case batch := <-la.processingChan:
			la.handleBatch(batch)
		case <-la.shutdownChan:
			for len(la.processingChan) > 0 {
				la.handleBatch(<-la.processingChan)
			}

			return
		}
	}
}

func (la *LogAggregator) handleBatch(batch *logBatch) {
	if len(batch.records) == 0 {
		return
	}

	counts := make(map[string]int64)
	for _, r := range batch.records {
		counts[r.AttributeValue]++
	}

	la.mu.Lock()
	for val, count := range counts {
		la.currentWindow.Add(val, count)
	}
	la.mu.Unlock()

	la.logger.DebugContext(batch.ctx, "Processed batch", "records", len(batch.records), "unique_values", len(counts))
}

func (la *LogAggregator) runRotator() {
	defer la.wg.Done()

	for {
		select {
		case <-la.windowTicker.C:
			la.rotateWindow()
		case <-la.shutdownChan:
			la.rotateWindow()

			return
		}
	}
}

// rotateWindow rotates the current window, logs the results, and resets the counter for the next window.
func (la *LogAggregator) rotateWindow() {
	la.mu.Lock()
	old := la.currentWindow
	la.currentWindow = NewAttributeCounter(time.Now().Add(la.config.WindowDuration))
	la.mu.Unlock()

	counts, totalLogs, totalBatches := old.Snapshot()

	if totalLogs == 0 {
		la.logger.Debug("Window rotated (no data)", "window_end", old.windowEnd.Format(time.RFC3339))

		return
	}

	la.logger.Info("Window results",
		"attribute_key", la.config.AttributeKey,
		"window_end", old.windowEnd.Format(time.RFC3339),
		"total_logs", totalLogs,
		"batches", totalBatches,
		"unique_values", len(counts))

	//nolint:forbidigo
	fmt.Printf("\n--- Aggregation (Window end: %s) ---\n", old.windowEnd.Format("2006-01-02 15:04:05"))

	//nolint:forbidigo
	fmt.Printf("Attribute: %s | Logs: %d | Batches: %d\n", la.config.AttributeKey, totalLogs, totalBatches)

	for val, count := range counts {
		//nolint:forbidigo
		fmt.Printf("  %q: %d\n", val, count)
	}

	//nolint:forbidigo
	fmt.Println("-----------------------------------")
}

// Shutdown stops the log aggregator, waits for all processing to finish, and closes the ticker.
func (la *LogAggregator) Shutdown() {
	close(la.shutdownChan)
	la.windowTicker.Stop()
	la.wg.Wait()
}

func (s *dash0LogsServiceServer) extractRecordsFromResourceLogs(rl *logspb.ResourceLogs) []LogRecord {
	var records []LogRecord

	resourceAttrs := extractAttributes(rl.GetResource())

	for _, sl := range rl.GetScopeLogs() {
		scopeAttrs := extractAttributes(sl.GetScope())

		for _, logRecord := range sl.GetLogRecords() {
			logAttrs := extractAttributes(logRecord)
			attrValue := s.findAttributeValue(logAttrs, scopeAttrs, resourceAttrs)
			body := extractLogBody(logRecord)

			timestamp := time.Now()
			if logRecord.GetTimeUnixNano() > 0 {
				//nolint:gosec
				timestamp = time.Unix(0, int64(logRecord.GetTimeUnixNano()))
			}

			records = append(records, LogRecord{
				AttributeValue: attrValue,
				Timestamp:      timestamp,
				Body:           body,
			})
		}
	}

	return records
}

// findAttributeValue returns the value for the configured attribute key from the provided attribute maps,
// checking them in order of precedence: log, scope, then resource.
func (s *dash0LogsServiceServer) findAttributeValue(attrs ...map[string]string) string {
	targetKey := s.processor.GetAttributeKey()

	for _, attr := range attrs {
		if v, ok := attr[targetKey]; ok {
			return v
		}
	}

	return UnknownAttributeValue
}

func extractAttributes(obj interface{}) map[string]string {
	attrs := make(map[string]string)

	var kvs []*commonpb.KeyValue

	switch logBody := obj.(type) {
	case *resourcepb.Resource:
		if logBody != nil {
			kvs = logBody.GetAttributes()
		}
	case *commonpb.InstrumentationScope:
		if logBody != nil {
			kvs = logBody.GetAttributes()
		}
	case *logspb.LogRecord:
		if logBody != nil {
			kvs = logBody.GetAttributes()
		}
	}

	for _, kv := range kvs {
		if val := getStringValue(kv.GetValue()); val != "" {
			attrs[kv.GetKey()] = val
		}
	}

	return attrs
}

func getStringValue(val *commonpb.AnyValue) string {
	if val == nil {
		return ""
	}

	switch valType := val.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return valType.StringValue
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(valType.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", valType.DoubleValue)
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(valType.BoolValue)
	default:
		return ""
	}
}

func extractLogBody(lr *logspb.LogRecord) string {
	if lr.GetBody() == nil {
		return ""
	}

	return getStringValue(lr.GetBody())
}
