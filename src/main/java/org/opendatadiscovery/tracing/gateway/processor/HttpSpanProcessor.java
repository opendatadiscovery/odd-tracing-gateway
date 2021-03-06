package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.trace.v1.Span;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.opendatadiscovery.adapter.contract.model.DataEntityType;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.HttpServicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.tracing.gateway.config.HttpProperties;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.opendatadiscovery.tracing.gateway.util.AnyValueUtil;
import org.opendatadiscovery.tracing.gateway.util.PathUtil;
import org.springframework.stereotype.Service;

import static org.opendatadiscovery.tracing.gateway.util.AnyValueUtil.toMap;

@Service
@AllArgsConstructor
public class HttpSpanProcessor implements SpanProcessor {
    private final Generator generator;
    private final HttpProperties httpProperties;

    @Override
    public boolean accept(final String library) {
        return library.startsWith("io.opentelemetry.netty")
            || library.startsWith("io.opentelemetry.jetty")
            || library.startsWith("io.opentelemetry.spring-webflux");
    }

    @Override
    public List<ServiceOddrns> process(final List<Span> spans,
                                       final Map<String, AnyValue> keyValue,
                                       final NameOddrn nameOddrn) {
        final Set<String> inputs = new HashSet<>();
        final Set<String> outputs = new HashSet<>();
        final List<ServiceOddrns> oddrns = new ArrayList<>();

        for (final Span span : spans) {
            final Map<String, AnyValue> attributes = toMap(span.getAttributesList());
            if (span.getKind().equals(Span.SpanKind.SPAN_KIND_SERVER)) {
                final Optional<String> host =
                    Optional.ofNullable(attributes.get("http.host")).map(AnyValue::getStringValue)
                        .map(PathUtil::replacePort);
                final Optional<String> method =
                    Optional.ofNullable(attributes.get("http.method")).map(AnyValue::getStringValue);
                final Optional<String> path =
                    Optional.ofNullable(attributes.get("http.target")).map(AnyValue::getStringValue);
                if (host.isPresent() && path.isPresent() && method.isPresent()) {
                    if (
                        (httpProperties.getStaticPrefix().isEmpty()
                            || !path.get().startsWith(httpProperties.getStaticPrefix()))
                            && (!httpProperties.isExcludeIps() || !PathUtil.isIp(host.get()))
                    ) {
                        final String sanitizedPath = PathUtil.sanitize(path.get());
                        final HttpServicePath httpPath = HttpServicePath.builder()
                            .host(host.get())
                            .method(method.get().toLowerCase())
                            .path(sanitizedPath)
                            .build();
                        final String groupOddrn = generateHost(
                            httpPath.toBuilder()
                                .method("")
                                .path("")
                                .build()
                        );

                        final String pathOddrn = generate(httpPath);
                        outputs.add(pathOddrn);

                        oddrns.add(
                            ServiceOddrns.builder()
                                .name(String.format("%s%s", host.get(), sanitizedPath))
                                .oddrn(pathOddrn)
                                .groupOddrn(groupOddrn)
                                .groupName(host.get())
                                .groupType(DataEntityType.API_SERVICE)
                                .serviceType(DataEntityType.API_CALL)
                                .inputs(Set.of())
                                .outputs(Set.of(nameOddrn.getOddrn()))
                                .metadata(
                                    Map.of(
                                        "http.host", host.get(),
                                        "http.method", method.get(),
                                        "http.path", PathUtil.sanitize(path.get())
                                    )
                                )
                                .build()
                        );

                        outputs.add(generate(httpPath));
                    }
                }
            } else if (span.getKind().equals(Span.SpanKind.SPAN_KIND_CLIENT)) {
                final Optional<URL> url =
                    Optional.ofNullable(attributes.get("http.url")).map(AnyValue::getStringValue)
                        .map(this::praseUrl);
                final Optional<String> method =
                    Optional.ofNullable(attributes.get("http.method")).map(AnyValue::getStringValue);
                if (url.isPresent() && method.isPresent()) {
                    if (httpProperties.isExcludeIps() && PathUtil.isIp(url.get().getHost())) {
                        continue;
                    }

                    final String sanitizePath = PathUtil.sanitize(url.get().getPath());
                    final HttpServicePath httpPath = HttpServicePath.builder()
                        .host(url.get().getHost())
                        .method(method.get().toLowerCase())
                        .path(sanitizePath)
                        .build();
                    final String pathOddrn = generate(httpPath);
                    final String groupOddrn = generateHost(
                        httpPath.toBuilder()
                            .method("")
                            .path("")
                            .build()
                    );

                    inputs.add(pathOddrn);

                    final Optional<Long> status =
                        Optional.ofNullable(attributes.get("http.status_code")).map(AnyValue::getIntValue);
                    if (status.isPresent() && (status.get() >= 100 && status.get() < 400)) {
                        oddrns.add(
                            ServiceOddrns.builder()
                                .name(String.format("%s%s", url.get().getHost(), sanitizePath))
                                .oddrn(pathOddrn)
                                .serviceType(DataEntityType.API_CALL)
                                .groupOddrn(groupOddrn)
                                .groupType(DataEntityType.API_SERVICE)
                                .groupName(url.get().getHost())
                                .inputs(Set.of())
                                .outputs(Set.of(nameOddrn.getOddrn()))
                                .metadata(
                                    Map.of(
                                        "http.host", url.get().getHost(),
                                        "http.method", method.get(),
                                        "http.path", sanitizePath
                                    )
                                )
                                .build()
                        );
                    }
                }
            }
        }

        oddrns.add(
            ServiceOddrns.builder()
                .inputs(inputs)
                .outputs(outputs)
                .oddrn(nameOddrn.getOddrn())
                .name(nameOddrn.getName())
                .version(nameOddrn.getVersion())
                .metadata(AnyValueUtil.toStringMap(keyValue))
                .build()
        );

        return oddrns;
    }

    @SneakyThrows
    private String generate(final OddrnPath path) {
        return generator.generate(path, "path");
    }

    @SneakyThrows
    private String generateHost(final OddrnPath path) {
        return generator.generate(path, "host");
    }

    @SneakyThrows
    private URL praseUrl(final String url) {
        return new URL(url);
    }
}
