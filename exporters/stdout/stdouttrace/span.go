package stdouttrace

import (
	"math"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Spans transforms a slice of OpenTelemetry spans into a slice of OTLP
// ResourceSpans.
func Spans(sdl []tracesdk.ReadOnlySpan) []*tracepb.ResourceSpans {
	if len(sdl) == 0 {
		return nil
	}

	rsm := make(map[attribute.Distinct]*tracepb.ResourceSpans)

	type key struct {
		r  attribute.Distinct
		is instrumentation.Scope
	}
	ssm := make(map[key]*tracepb.ScopeSpans)

	var resources int
	for _, sd := range sdl {
		if sd == nil {
			continue
		}

		rKey := sd.Resource().Equivalent()
		k := key{
			r:  rKey,
			is: sd.InstrumentationScope(),
		}
		scopeSpan, iOk := ssm[k]
		if !iOk {
			// Either the resource or instrumentation scope were unknown.
			scopeSpan = &tracepb.ScopeSpans{
				Scope:     InstrumentationScope(sd.InstrumentationScope()),
				Spans:     []*tracepb.Span{},
				SchemaUrl: sd.InstrumentationScope().SchemaURL,
			}
		}
		scopeSpan.Spans = append(scopeSpan.Spans, span(sd))
		ssm[k] = scopeSpan

		rs, rOk := rsm[rKey]
		if !rOk {
			resources++
			// The resource was unknown.
			rs = &tracepb.ResourceSpans{
				Resource:   Resource(sd.Resource()),
				ScopeSpans: []*tracepb.ScopeSpans{scopeSpan},
				SchemaUrl:  sd.Resource().SchemaURL(),
			}
			rsm[rKey] = rs
			continue
		}

		// The resource has been seen before. Check if the instrumentation
		// library lookup was unknown because if so we need to add it to the
		// ResourceSpans. Otherwise, the instrumentation library has already
		// been seen and the append we did above will be included it in the
		// ScopeSpans reference.
		if !iOk {
			rs.ScopeSpans = append(rs.ScopeSpans, scopeSpan)
		}
	}

	// Transform the categorized map into a slice
	rss := make([]*tracepb.ResourceSpans, 0, resources)
	for _, rs := range rsm {
		rss = append(rss, rs)
	}
	return rss
}

// Resource transforms a Resource into an OTLP Resource.
func Resource(r *resource.Resource) *resourcepb.Resource {
	if r == nil {
		return nil
	}
	return &resourcepb.Resource{Attributes: ResourceAttributes(r)}
}

// ResourceAttributes transforms a Resource OTLP key-values.
func ResourceAttributes(res *resource.Resource) []*commonpb.KeyValue {
	return Iterator(res.Iter())
}

// span transforms a Span into an OTLP span.
func span(sd tracesdk.ReadOnlySpan) *tracepb.Span {
	if sd == nil {
		return nil
	}

	tid := sd.SpanContext().TraceID()
	sid := sd.SpanContext().SpanID()

	s := &tracepb.Span{
		TraceId:                tid[:],
		SpanId:                 sid[:],
		TraceState:             sd.SpanContext().TraceState().String(),
		Status:                 status(sd.Status().Code, sd.Status().Description),
		StartTimeUnixNano:      uint64(max(0, sd.StartTime().UnixNano())), // nolint:gosec // Overflow checked.
		EndTimeUnixNano:        uint64(max(0, sd.EndTime().UnixNano())),   // nolint:gosec // Overflow checked.
		Links:                  links(sd.Links()),
		Kind:                   spanKind(sd.SpanKind()),
		Name:                   sd.Name(),
		Attributes:             KeyValues(sd.Attributes()),
		Events:                 spanEvents(sd.Events()),
		DroppedAttributesCount: clampUint32(sd.DroppedAttributes()),
		DroppedEventsCount:     clampUint32(sd.DroppedEvents()),
		DroppedLinksCount:      clampUint32(sd.DroppedLinks()),
	}

	if psid := sd.Parent().SpanID(); psid.IsValid() {
		s.ParentSpanId = psid[:]
	}
	s.Flags = buildSpanFlags(sd.Parent())

	return s
}

func clampUint32(v int) uint32 {
	if v < 0 {
		return 0
	}
	if int64(v) > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v) // nolint: gosec  // Overflow/Underflow checked.
}

// status transform a span code and message into an OTLP span status.
func status(status codes.Code, message string) *tracepb.Status {
	var c tracepb.Status_StatusCode
	switch status {
	case codes.Ok:
		c = tracepb.Status_STATUS_CODE_OK
	case codes.Error:
		c = tracepb.Status_STATUS_CODE_ERROR
	default:
		c = tracepb.Status_STATUS_CODE_UNSET
	}
	return &tracepb.Status{
		Code:    c,
		Message: message,
	}
}

// links transforms span Links to OTLP span links.
func links(links []tracesdk.Link) []*tracepb.Span_Link {
	if len(links) == 0 {
		return nil
	}

	sl := make([]*tracepb.Span_Link, 0, len(links))
	for _, otLink := range links {
		// This redefinition is necessary to prevent otLink.*ID[:] copies
		// being reused -- in short we need a new otLink per iteration.
		otLink := otLink

		tid := otLink.SpanContext.TraceID()
		sid := otLink.SpanContext.SpanID()

		flags := buildSpanFlags(otLink.SpanContext)

		sl = append(sl, &tracepb.Span_Link{
			TraceId:                tid[:],
			SpanId:                 sid[:],
			Attributes:             KeyValues(otLink.Attributes),
			DroppedAttributesCount: clampUint32(otLink.DroppedAttributeCount),
			Flags:                  flags,
		})
	}
	return sl
}

func buildSpanFlags(sc trace.SpanContext) uint32 {
	flags := tracepb.SpanFlags_SPAN_FLAGS_CONTEXT_HAS_IS_REMOTE_MASK
	if sc.IsRemote() {
		flags |= tracepb.SpanFlags_SPAN_FLAGS_CONTEXT_IS_REMOTE_MASK
	}

	return uint32(flags) // nolint:gosec // Flags is a bitmask and can't be negative
}

// spanEvents transforms span Events to an OTLP span events.
func spanEvents(es []tracesdk.Event) []*tracepb.Span_Event {
	if len(es) == 0 {
		return nil
	}

	events := make([]*tracepb.Span_Event, len(es))
	// Transform message events
	for i := 0; i < len(es); i++ {
		events[i] = &tracepb.Span_Event{
			Name:                   es[i].Name,
			TimeUnixNano:           uint64(max(0, es[i].Time.UnixNano())), // nolint:gosec // Overflow checked.
			Attributes:             KeyValues(es[i].Attributes),
			DroppedAttributesCount: clampUint32(es[i].DroppedAttributeCount),
		}
	}
	return events
}

// spanKind transforms a SpanKind to an OTLP span kind.
func spanKind(kind trace.SpanKind) tracepb.Span_SpanKind {
	switch kind {
	case trace.SpanKindInternal:
		return tracepb.Span_SPAN_KIND_INTERNAL
	case trace.SpanKindClient:
		return tracepb.Span_SPAN_KIND_CLIENT
	case trace.SpanKindServer:
		return tracepb.Span_SPAN_KIND_SERVER
	case trace.SpanKindProducer:
		return tracepb.Span_SPAN_KIND_PRODUCER
	case trace.SpanKindConsumer:
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

func InstrumentationScope(il instrumentation.Scope) *commonpb.InstrumentationScope {
	if il == (instrumentation.Scope{}) {
		return nil
	}
	return &commonpb.InstrumentationScope{
		Name:       il.Name,
		Version:    il.Version,
		Attributes: Iterator(il.Attributes.Iter()),
	}
}

// KeyValues transforms a slice of attribute KeyValues into OTLP key-values.
func KeyValues(attrs []attribute.KeyValue) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return nil
	}

	out := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, kv := range attrs {
		out = append(out, KeyValue(kv))
	}
	return out
}

// KeyValue transforms an attribute KeyValue into an OTLP key-value.
func KeyValue(kv attribute.KeyValue) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: string(kv.Key), Value: Value(kv.Value)}
}

// Value transforms an attribute Value into an OTLP AnyValue.
func Value(v attribute.Value) *commonpb.AnyValue {
	av := new(commonpb.AnyValue)
	switch v.Type() {
	case attribute.BOOL:
		av.Value = &commonpb.AnyValue_BoolValue{
			BoolValue: v.AsBool(),
		}
	case attribute.BOOLSLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: boolSliceValues(v.AsBoolSlice()),
			},
		}
	case attribute.INT64:
		av.Value = &commonpb.AnyValue_IntValue{
			IntValue: v.AsInt64(),
		}
	case attribute.INT64SLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: int64SliceValues(v.AsInt64Slice()),
			},
		}
	case attribute.FLOAT64:
		av.Value = &commonpb.AnyValue_DoubleValue{
			DoubleValue: v.AsFloat64(),
		}
	case attribute.FLOAT64SLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: float64SliceValues(v.AsFloat64Slice()),
			},
		}
	case attribute.STRING:
		av.Value = &commonpb.AnyValue_StringValue{
			StringValue: v.AsString(),
		}
	case attribute.STRINGSLICE:
		av.Value = &commonpb.AnyValue_ArrayValue{
			ArrayValue: &commonpb.ArrayValue{
				Values: stringSliceValues(v.AsStringSlice()),
			},
		}
	default:
		av.Value = &commonpb.AnyValue_StringValue{
			StringValue: "INVALID",
		}
	}
	return av
}

func boolSliceValues(vals []bool) []*commonpb.AnyValue {
	converted := make([]*commonpb.AnyValue, len(vals))
	for i, v := range vals {
		converted[i] = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_BoolValue{
				BoolValue: v,
			},
		}
	}
	return converted
}

func int64SliceValues(vals []int64) []*commonpb.AnyValue {
	converted := make([]*commonpb.AnyValue, len(vals))
	for i, v := range vals {
		converted[i] = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_IntValue{
				IntValue: v,
			},
		}
	}
	return converted
}

func float64SliceValues(vals []float64) []*commonpb.AnyValue {
	converted := make([]*commonpb.AnyValue, len(vals))
	for i, v := range vals {
		converted[i] = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_DoubleValue{
				DoubleValue: v,
			},
		}
	}
	return converted
}

func stringSliceValues(vals []string) []*commonpb.AnyValue {
	converted := make([]*commonpb.AnyValue, len(vals))
	for i, v := range vals {
		converted[i] = &commonpb.AnyValue{
			Value: &commonpb.AnyValue_StringValue{
				StringValue: v,
			},
		}
	}
	return converted
}

// Iterator transforms an attribute iterator into OTLP key-values.
func Iterator(iter attribute.Iterator) []*commonpb.KeyValue {
	l := iter.Len()
	if l == 0 {
		return nil
	}

	out := make([]*commonpb.KeyValue, 0, l)
	for iter.Next() {
		out = append(out, KeyValue(iter.Attribute()))
	}
	return out
}
