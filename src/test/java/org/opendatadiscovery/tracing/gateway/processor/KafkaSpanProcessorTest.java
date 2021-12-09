package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.trace.v1.Span;
import io.opentelemetry.proto.trace.v1.Status;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.tracing.gateway.config.AppProperties;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opendatadiscovery.tracing.gateway.util.KeyValueUtils.withString;

public class KafkaSpanProcessorTest {
    private final KafkaSpanProcessor processor =
        new KafkaSpanProcessor(new Generator(), new AppProperties());

    @Test
    public void testServer() {
        final Instant now = Instant.now();
        final List<ServiceOddrns> oddrns = processor.process(
            List.of(
                Span.newBuilder()
                    .setKind(Span.SpanKind.SPAN_KIND_PRODUCER)
                    .setName("Kafka CALL")
                    .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build())
                    .addAllAttributes(
                        List.of(
                            withString("messaging.destination", "topic")
                        )
                    ).build()
            ), Map.of(), NameOddrn.builder().oddrn("").name("").build()
        ).stream().map(s -> s.toBuilder().updatedAt(now).build()).collect(Collectors.toList());
        assertEquals(
            List.of(
                ServiceOddrns.builder()
                    .updatedAt(now)
                    .inputs(Set.of())
                    .outputs(
                        Set.of(
                            "//kafka/host/unknown/topics/topic"
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
                    .setKind(Span.SpanKind.SPAN_KIND_CONSUMER)
                    .setName("Kafka CALL")
                    .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build())
                    .addAllAttributes(
                        List.of(
                            withString("messaging.destination", "topic")
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
                            "//kafka/host/unknown/topics/topic"
                        )
                    ).build()
            ), oddrns
        );
    }
}