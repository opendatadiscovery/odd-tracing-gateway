package org.opendatadiscovery.tracing.gateway.model;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class ServiceOddrns {
    @Builder.Default
    private final String name = "";
    @Builder.Default
    private final Instant updatedAt = Instant.now();
    @Builder.Default
    private final Map<String, String> metadata = Map.of();
    private final Set<String> inputs;
    private final Set<String> outputs;
}
