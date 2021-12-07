package org.opendatadiscovery.tracing.gateway.db;

import java.util.List;
import java.util.regex.Pattern;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class PostgreSqlParser extends SqlParser {
    @Override
    public List<Tuple2<Pattern, String>> hooks() {
        return List.of(
            // WITH query AS MATERIALIZED ( -> WITH query AS (
            Tuples.of(
                Pattern.compile(
                    "(,|with|WITH|With)\\s+([a-zA-Z0-9_\"]+)\\s+(as|AS) (materialized|MATERIALIZED)\\s*\\("
                ), "$1 $2 AS ("
            ),
            // method(schema.table.*) -> method(schema.table."ODD_ALL_COLUMNS_WILDCARD")
            Tuples.of(
                Pattern.compile("([a-zA-Z0-9_\"\\.]+)\\(([a-zA-Z0-9_\"\\.]+)\\.\\*\\)"),
                "$1($2.\"ODD_ALL_COLUMNS_WILDCARD\")"
            ),
            // INSERT INTO table(columns) VALUES (values) ON CONFLICT DO -> INSERT INTO table(columns) VALUES (values);
            Tuples.of(
                Pattern.compile("(ON CONFLICT.+)\\s+RETURNING (.+)", Pattern.CASE_INSENSITIVE),
                "RETURNING $2"
            ),
            Tuples.of(
                Pattern.compile("(ON CONFLICT.+)", Pattern.CASE_INSENSITIVE),
                ""
            )
        );
    }
}
