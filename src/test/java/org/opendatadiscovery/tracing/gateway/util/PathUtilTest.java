package org.opendatadiscovery.tracing.gateway.util;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PathUtilTest {
    @TestFactory
    public Iterable<DynamicTest> testSql() {
        return List.of(
            compare(
                "/",
                "/"
            ),
            compare(
                "/ingestion/datasources/active",
                "/ingestion/datasources/active"
            ),
            compare(
                "/ingestion/datasources/" + ThreadLocalRandom.current().nextLong(),
                "/ingestion/datasources/{number}"
            ),
            compare(
                "/ingestion/datasources/" + ThreadLocalRandom.current().nextDouble(),
                "/ingestion/datasources/{number}"
            ),
            compare(
                "/ingestion/datasources/" + UUID.randomUUID() + "/",
                "/ingestion/datasources/{uuid}"
            ),
            compare(
                "/?field=value",
                "/"
            ),
            compare(
                "/ingestion/datasources/active?field=value",
                "/ingestion/datasources/active"
            ),
            compare(
                "/ingestion/datasources/" + ThreadLocalRandom.current().nextLong() + "?field=value",
                "/ingestion/datasources/{number}"
            ),
            compare(
                "/ingestion/datasources/" + ThreadLocalRandom.current().nextDouble() +  "?field=value",
                "/ingestion/datasources/{number}"
            ),
            compare(
                "/ingestion/datasources/" + UUID.randomUUID() + "/?field=value",
                "/ingestion/datasources/{uuid}"
            )
        );
    }

    public DynamicTest compare(final String input, final String expected) {
        return DynamicTest.dynamicTest(input,
            () -> {
                final String sanitize = PathUtil.sanitize(input);
                assertEquals(expected, sanitize);
            }
        );
    }
}