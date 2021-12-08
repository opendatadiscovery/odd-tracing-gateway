package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.trace.v1.Span;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.KafkaPath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.tracing.gateway.config.AppProperties;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.springframework.stereotype.Service;

import static org.opendatadiscovery.tracing.gateway.util.AnyValueUtil.toMap;

@Service
@AllArgsConstructor
public class KafkaSpanProcessor implements SpanProcessor {
    private final Generator generator;
    private final AppProperties properties;

    @Override
    public boolean accept(final String library) {
        return library.startsWith("io.opentelemetry.kafka-clients");
    }

    @Override
    public ServiceOddrns process(final List<Span> spans, final Map<String, AnyValue> keyValue) {
        final Set<String> inputs = new HashSet<>();
        final Set<String> outputs = new HashSet<>();

        for (final Span span : spans) {
            final Map<String, AnyValue> attributes = toMap(span.getAttributesList());
            final String host = Optional.ofNullable(attributes.get("messaging.url"))
                .map(u -> praseUrl(u.getStringValue()))
                .map(URL::getHost).orElse(properties.getKafkaServers());
            final Optional<String> destination = Optional.ofNullable(
                attributes.get("messaging.destination")
            ).map(AnyValue::getStringValue);

            if (destination.isPresent()) {
                final KafkaPath path = KafkaPath.builder()
                    .host(host)
                    .topic(destination.get())
                    .build();
                if (span.getKind().equals(Span.SpanKind.SPAN_KIND_CONSUMER)) {
                    inputs.add(generate(path));
                } else if (span.getKind().equals(Span.SpanKind.SPAN_KIND_PRODUCER)) {
                    outputs.add(generate(path));
                }
            }
        }
        return ServiceOddrns.builder()
            .inputs(inputs)
            .outputs(outputs)
            .build();
    }

    @SneakyThrows
    private String generate(final OddrnPath path) {
        return generator.generate(path, "topic");
    }

    @SneakyThrows
    private URL praseUrl(final String url) {
        return new URL(url);
    }
}
