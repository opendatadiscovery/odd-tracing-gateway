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
import static org.opendatadiscovery.tracing.gateway.processor.KeyValueUtils.withInt;
import static org.opendatadiscovery.tracing.gateway.processor.KeyValueUtils.withString;

public class JdbcSpanProcessorTest {
    private final JdbcSpanProcessor processor = new JdbcSpanProcessor(new Generator());

    @Test
    public void test() {
        final Instant now = Instant.now();
        final ServiceOddrns oddrns = processor.process(
            List.of(
                Span.newBuilder()
                    .setKind(Span.SpanKind.SPAN_KIND_CLIENT)
                    .setStatus(Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build())
                    .addAllAttributes(
                        List.of(
                            withString("db.system", "postgresql"),
                            withString("db.statement",
                                "insert into \"public\".\"client\" (\"name\") values (?) returning "
                                    + "\"public\".\"client\".\"id\", \"public\".\"client\".\"name\", "
                                    + "\"public\".\"client\".\"is_deleted\", \"public\".\"client\".\"created_at\", "
                                    + "\"public\".\"client\".\"updated_at\""
                            ),
                            withString("db.user", "odd-traces-test-app"),
                            withInt("thread.id", 58),
                            withString("db.connection_string", "postgresql://database:5432"),
                            withString("net.peer.name", "database"),
                            withString("db.name", "odd_traces_test_app"),
                            withString("thread.name", "reactor-http-epoll-3"),
                            withInt("net.peer.port", 5432)
                        )
                    ).build()
            ), Map.of()
        ).toBuilder().updatedAt(now).build();
        assertEquals(
            ServiceOddrns.builder()
                .updatedAt(now)
                .outputs(
                    Set.of(
                        "//postgresql/host/database/databases/odd_traces_test_app/schemas/public/tables/client"
                    )
                ).inputs(Set.of()).build(),
            oddrns
        );
    }
}