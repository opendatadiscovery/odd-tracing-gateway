package org.opendatadiscovery.tracing.gateway.resolver;

import io.opentelemetry.proto.common.v1.AnyValue;
import java.util.Map;
import java.util.Optional;

public interface ServiceNameResolver {
    int priority();

    Optional<String> resolve(Map<String, AnyValue> resourceMap);
}
