package org.opendatadiscovery.tracing.gateway.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder(toBuilder = true)
public class NameOddrn {
    private final String oddrn;
    private final String name;
    @Builder.Default
    private final String version = "unknown";
}
