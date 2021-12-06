package org.opendatadiscovery.tracing.gateway.resolver;

import io.opentelemetry.proto.common.v1.AnyValue;
import java.util.Map;
import java.util.Optional;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;

public interface ServiceNameResolver {
    int priority();

    Optional<NameOddrn> resolve(Map<String, AnyValue> resourceMap);
}
