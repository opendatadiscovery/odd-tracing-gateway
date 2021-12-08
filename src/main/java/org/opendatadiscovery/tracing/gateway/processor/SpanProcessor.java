package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.List;
import java.util.Map;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;

public interface SpanProcessor {
    boolean accept(String library);

    ServiceOddrns process(List<Span> spans, Map<String, AnyValue> keyValue);
}
