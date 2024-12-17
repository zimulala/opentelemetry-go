// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package stdouttrace // import "go.opentelemetry.io/otel/exporters/stdout/stdouttrace"

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

var zeroTime time.Time

var _ trace.SpanExporter = &Exporter{}

// New creates an Exporter with the passed options.
func New(options ...Option) (*Exporter, error) {
	fmt.Println("yyy-------------------------------- New Exporter")
	cfg := newConfig(options...)

	enc := json.NewEncoder(cfg.Writer)
	if cfg.PrettyPrint {
		enc.SetIndent("", "\t")
	}

	return &Exporter{
		encoder:    enc,
		timestamps: cfg.Timestamps,
	}, nil
}

// Exporter is an implementation of trace.SpanSyncer that writes spans to stdout.
type Exporter struct {
	encoder    *json.Encoder
	encoderMu  sync.Mutex
	timestamps bool

	stoppedMu sync.RWMutex
	stopped   bool
}

// ExportSpans writes spans in json format to stdout.
func (e *Exporter) ExportSpans(ctx context.Context, spans []trace.ReadOnlySpan) error {
	/*
		fmt.Println("yyy-------------------------------- ExportSpans, proto spans")
		protoSpans := Spans(spans)
		if len(protoSpans) == 0 {
			return nil
		}
		for _, sp := range protoSpans {
			fmt.Printf("yyy-------------------------------- ExportSpans 00, protoSpan:%#v\n", sp)
			for _, scopSp := range sp.ScopeSpans {
				fmt.Printf("yyy-------------------------------- ExportSpans 11, scopSpan:%#v, spans:%#v\n", scopSp, scopSp.Spans)
				for _, s := range scopSp.Spans {
					for _, e := range s.Events {
						fmt.Printf("yyy-------------------------------- ExportSpans 22, span:%#v, event:%#v\n", s, e)
					}
				}
			}
		}
	*/

	fmt.Println("yyy-------------------------------- ExportSpans, stdout")
	if err := ctx.Err(); err != nil {
		return err
	}
	e.stoppedMu.RLock()
	stopped := e.stopped
	e.stoppedMu.RUnlock()
	if stopped {
		return nil
	}

	if len(spans) == 0 {
		return nil
	}

	stubs := tracetest.SpanStubsFromReadOnlySpans(spans)

	e.encoderMu.Lock()
	defer e.encoderMu.Unlock()
	for i := range stubs {
		stub := &stubs[i]
		// Remove timestamps
		if !e.timestamps {
			stub.StartTime = zeroTime
			stub.EndTime = zeroTime
			for j := range stub.Events {
				ev := &stub.Events[j]
				ev.Time = zeroTime
			}
		}

		fmt.Println("yyy-------------------------------- ExportSpans, encode 10")
		// Encode span stubs, one by one
		if err := e.encoder.Encode(stub); err != nil {
			return err
		}
		fmt.Println("yyy-------------------------------- ExportSpans, encode 11")
	}
	return nil
}

// Shutdown is called to stop the exporter, it performs no action.
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.stoppedMu.Lock()
	e.stopped = true
	e.stoppedMu.Unlock()

	return nil
}

// MarshalLog is the marshaling function used by the logging system to represent this Exporter.
func (e *Exporter) MarshalLog() interface{} {
	return struct {
		Type           string
		WithTimestamps bool
	}{
		Type:           "stdout",
		WithTimestamps: e.timestamps,
	}
}
