package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opendatadiscovery.adapter.contract.model.DataEntityType;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opendatadiscovery.tracing.gateway.util.KeyValueUtils.withInt;
import static org.opendatadiscovery.tracing.gateway.util.KeyValueUtils.withString;

public class HttpSpanProcessorTest {
    private final HttpSpanProcessor processor = new HttpSpanProcessor(new Generator());

    @Test
    public void testServer() {
        final Instant now = Instant.now();
        final Set<ServiceOddrns> oddrns = processor.process(
            List.of(
                Span.newBuilder()
                    .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                    .setName("/ingestion/datasources/active")
                    .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build())
                    .addAllAttributes(
                        List.of(
                            withString("http.scheme", "http"),
                            withString("http.host", "odd-platform"),
                            withInt("thread.id", 32),
                            withString("net.peer.ip", "10.7.154.205"),
                            withString("thread.name", "reactor-http-epoll-3"),
                            withString("http.method", "GET"),
                            withInt("http.status_code", 200),
                            withString("net.peer.name", "10-7-154-205.odd-platform-puller.demo.svc.cluster.local"),
                            withString("http.user_agent", "ReactorNetty/1.0.8"),
                            withString("http.flavor", "1.1"),
                            withString("http.target", "/ingestion/datasources/active/?field=value"),
                            withInt("net.peer.port", 55286)
                        )
                    ).build()
            ), Map.of(), NameOddrn.builder().oddrn("//microservice/1").name("").build()
        ).stream().map(s -> s.toBuilder().updatedAt(now).build()).collect(Collectors.toSet());
        assertEquals(
            Set.of(
                ServiceOddrns.builder()
                    .serviceType(DataEntityType.MICROSERVICE)
                    .oddrn("//microservice/1")
                    .updatedAt(now)
                    .inputs(Set.of())
                    .outputs(
                        Set.of(
                            "//http/host/odd-platform/method/get/path/\\\\ingestion\\\\datasources\\\\active"
                        )
                    ).build(),
                ServiceOddrns.builder()
                    .name("odd-platform/ingestion/datasources/active")
                    .oddrn("//http/host/odd-platform/method/get/path/\\\\ingestion\\\\datasources\\\\active")
                    .serviceType(DataEntityType.API_CALL)
                    .updatedAt(now)
                    .metadata(
                        Map.of(
                            "http.host", "odd-platform",
                            "http.method", "GET",
                            "http.path", "/ingestion/datasources/active"
                        )
                    )
                    .inputs(Set.of())
                    .outputs(
                        Set.of(
                            "//microservice/1"
                        )
                    ).build()
            ),
            oddrns
        );
    }

    @Test
    public void testClient() {
        final Instant now = Instant.now();
        final List<ServiceOddrns> oddrns = processor.process(
            List.of(
                Span.newBuilder()
                    .setKind(Span.SpanKind.SPAN_KIND_CLIENT)
                    .setName("HTTP GET")
                    .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build())
                    .addAllAttributes(
                        List.of(
                            withString("http.url", "http://odd-platform:80/ingestion/datasources/active"),
                            withInt("net.peer.port", 80),
                            withString("thread.name", "reactor-http-epoll-3"),
                            withInt("http.status_code", 200),
                            withString("http.method", "GET"),
                            withInt("thread.id", 32),
                            withString("net.transport", "ip_tcp"),
                            withString("net.peer.name", "odd-platform")
                        )
                    ).build()
            ), Map.of(), NameOddrn.builder().oddrn("").name("").build()
        ).stream().map(s -> s.toBuilder().updatedAt(now).build()).collect(Collectors.toList());
        assertEquals(
            List.of(
                ServiceOddrns.builder()
                    .updatedAt(now)
                    .outputs(Set.of())
                    .inputs(
                        Set.of(
                            "//http/host/odd-platform/method/get/path/\\\\ingestion\\\\datasources\\\\active"
                        )
                    ).build()
            ), oddrns
        );
    }
}