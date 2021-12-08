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
import org.opendatadiscovery.oddrn.model.HttpServicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.opendatadiscovery.tracing.gateway.util.PathUtil;
import org.springframework.stereotype.Service;

import static org.opendatadiscovery.tracing.gateway.util.AnyValueUtil.toMap;

@Service
@AllArgsConstructor
public class HttpSpanProcessor implements SpanProcessor {
    private final Generator generator;

    @Override
    public boolean accept(final String library) {
        return library.startsWith("io.opentelemetry.netty")
            || library.startsWith("io.opentelemetry.jetty")
            || library.startsWith("io.opentelemetry.spring-webflux");
    }

    @Override
    public ServiceOddrns process(final List<Span> spans, final Map<String, AnyValue> keyValue) {
        final Set<String> inputs = new HashSet<>();
        final Set<String> outputs = new HashSet<>();

        for (final Span span : spans) {
            final Map<String, AnyValue> attributes = toMap(span.getAttributesList());
            if (span.getKind().equals(Span.SpanKind.SPAN_KIND_SERVER)) {
                final Optional<String> host =
                    Optional.ofNullable(attributes.get("http.host")).map(AnyValue::getStringValue);
                final Optional<String> method =
                    Optional.ofNullable(attributes.get("http.method")).map(AnyValue::getStringValue);
                final Optional<String> path =
                    Optional.ofNullable(attributes.get("http.target")).map(AnyValue::getStringValue);
                if (host.isPresent() && path.isPresent() && method.isPresent()) {
                    final HttpServicePath httpPath = HttpServicePath.builder()
                        .host(host.get())
                        .method(method.get().toLowerCase())
                        .path(PathUtil.sanitize(path.get()))
                        .build();
                    outputs.add(generate(httpPath));
                }
            } else if (span.getKind().equals(Span.SpanKind.SPAN_KIND_CLIENT)) {
                final Optional<URL> url =
                    Optional.ofNullable(attributes.get("http.url")).map(AnyValue::getStringValue)
                        .map(this::praseUrl);
                final Optional<String> method =
                    Optional.ofNullable(attributes.get("http.method")).map(AnyValue::getStringValue);
                if (url.isPresent() && method.isPresent()) {
                    final HttpServicePath httpPath = HttpServicePath.builder()
                        .host(url.get().getHost())
                        .method(method.get().toLowerCase())
                        .path(PathUtil.sanitize(url.get().getPath()))
                        .build();
                    inputs.add(generate(httpPath));
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
        return generator.generate(path, "path");
    }

    @SneakyThrows
    private URL praseUrl(final String url) {
        return new URL(url);
    }
}
