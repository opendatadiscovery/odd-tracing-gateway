package org.opendatadiscovery.tracing.gateway.resolver;

import io.opentelemetry.proto.common.v1.AnyValue;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.NamedMicroservicePath;
import org.opendatadiscovery.tracing.gateway.config.AppProperties;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.springframework.stereotype.Component;

import static org.opendatadiscovery.tracing.gateway.util.VersionUtil.parseName;

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
            .map(AnyValue::getStringValue)
            .map(this::serialize);
    }

    @SneakyThrows
    private NameOddrn serialize(final String path) {
        final NameOddrn name = parseName(
            path
        );
        return name.toBuilder()
            .oddrn(
                generator.generate(
                    NamedMicroservicePath.builder().name(name.getName()).build(),
                    "name"
                )
            )
            .build();
    }
}
