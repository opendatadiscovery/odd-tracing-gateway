package org.opendatadiscovery.tracing.gateway.resolver;

import io.opentelemetry.proto.common.v1.AnyValue;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.NamedMicroservicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.tracing.gateway.config.AppProperties;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class DefaultServiceNameResolver implements ServiceNameResolver {
    private final Generator generator;
    private final AppProperties properties;

    @Override
    public int priority() {
        return properties.getDefaultNamePriority();
    }

    @Override
    public Optional<NameOddrn> resolve(final Map<String, AnyValue> resourceMap) {
        return Optional.ofNullable(resourceMap.get("service.name"))
            .map(n -> NamedMicroservicePath.builder().name(n.getStringValue()).build())
            .map(this::serialize);
    }

    @SneakyThrows
    private NameOddrn serialize(final NamedMicroservicePath path) {
        return NameOddrn.builder()
            .name(path.getName())
            .oddrn(generator.generate(path, "name"))
            .build();
    }
}
