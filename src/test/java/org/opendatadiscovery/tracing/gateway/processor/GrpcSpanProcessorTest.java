package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opendatadiscovery.tracing.gateway.util.KeyValueUtils.withString;

public class GrpcSpanProcessorTest {
    private final GrpcSpanProcessor processor = new GrpcSpanProcessor(new Generator());

    @Test
    public void testServer() {
        final Instant now = Instant.now();
        final ServiceOddrns oddrns = processor.process(
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
            ), Map.of()
        ).toBuilder().updatedAt(now).build();
        assertEquals(
            ServiceOddrns.builder()
                .updatedAt(now)
                .inputs(Set.of())
                .outputs(
                    Set.of(
                        "//grpc/host/empty/services/com.odd.Grpc/methods/Health"
                    )
                ).build(),
            oddrns
        );
    }

    @Test
    public void testClient() {
        final Instant now = Instant.now();
        final ServiceOddrns oddrns = processor.process(
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
            ), Map.of()
        ).toBuilder().updatedAt(now).build();
        assertEquals(
            ServiceOddrns.builder()
                .updatedAt(now)
                .outputs(Set.of())
                .inputs(
                    Set.of(
                        "//grpc/host/empty/services/com.odd.Grpc/methods/Health"
                    )
                ).build(),
            oddrns
        );
    }
}