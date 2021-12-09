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
import static org.opendatadiscovery.tracing.gateway.util.KeyValueUtils.withString;

public class GrpcSpanProcessorTest {
    private final GrpcSpanProcessor processor = new GrpcSpanProcessor(new Generator());

    @Test
    public void testServer() {
        final Instant now = Instant.now();
        final Set<ServiceOddrns> oddrns = processor.process(
            List.of(
                Span.newBuilder()
                    .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                    .setName("GRPC CALL")
                    .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build())
                    .addAllAttributes(
                        List.of(
                            withString("rpc.service", "com.odd.Grpc"),
                            withString("rpc.method", "Health")
                        )
                    ).build()
            ), Map.of(), NameOddrn.builder().oddrn("//microservice/1").name("").build()
        ).stream().map(s -> s.toBuilder().updatedAt(now).build()).collect(Collectors.toSet());
        assertEquals(
            Set.of(
                ServiceOddrns.builder()
                    .oddrn("//microservice/1")
                    .serviceType(DataEntityType.MICROSERVICE)
                    .updatedAt(now)
                    .inputs(Set.of())
                    .outputs(
                        Set.of(
                            "//grpc/host/empty/services/com.odd.Grpc/methods/Health"
                        )
                    ).build(),
                ServiceOddrns.builder()
                    .name("com.odd.Grpc/Health")
                    .oddrn("//grpc/host/empty/services/com.odd.Grpc/methods/Health")
                    .serviceType(DataEntityType.API_CALL)
                    .updatedAt(now)
                    .metadata(
                        Map.of(
                            "rpc.service", "com.odd.Grpc",
                            "rpc.method", "Health"
                        )
                    )
                    .inputs(Set.of())
                    .outputs(
                        Set.of(
                            "//microservice/1"
                        )
                    ).build()
            ), oddrns
        );
    }

    @Test
    public void testClient() {
        final Instant now = Instant.now();
        final List<ServiceOddrns> oddrns = processor.process(
            List.of(
                Span.newBuilder()
                    .setKind(Span.SpanKind.SPAN_KIND_CLIENT)
                    .setName("GRPC CALL")
                    .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build())
                    .addAllAttributes(
                        List.of(
                            withString("rpc.service", "com.odd.Grpc"),
                            withString("rpc.method", "Health")
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
                            "//grpc/host/empty/services/com.odd.Grpc/methods/Health"
                        )
                    ).build()
            ),
            oddrns
        );
    }
}