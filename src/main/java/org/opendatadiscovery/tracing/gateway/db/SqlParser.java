package org.opendatadiscovery.tracing.gateway.db;

import java.util.stream.Collectors;
import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class SqlParser {
    @SneakyThrows
    public static SqlStatementInfo parse(final String statement) {
        final Statement stmt = CCJSqlParserUtil.parse(
            removeMaterialized(statement)
        );
        final SqlParserVisitor visitor = new SqlParserVisitor();
        stmt.accept(visitor);
        return new SqlStatementInfo(
            visitor.getInputTables().stream()
                .map(t -> new TableName(t.getSchemaName(), t.getName()))
                .collect(Collectors.toSet()),
            visitor.getOutputTables().stream()
                .map(t -> new TableName(t.getSchemaName(), t.getName()))
                .collect(Collectors.toSet())
        );
    }

    public static String removeMaterialized(final String statement) {
        // with\s*([0-9A-Za-z_\"]+) as materialized\s*\(
        /* as materialized ( */
        return statement.replaceAll(
            "(with|WITH|With)\\s+([a-zA-Z0-9_\"]+)\\s+(as|AS) (materialized|MATERIALIZED)\\s*\\(",
            "with $2 AS ("
        ).replaceAll(
            "([a-zA-Z0-9_\"\\.]+)\\(([a-zA-Z0-9_\"\\.]+)\\.\\*\\)",
            "$1($2.\"ALL_COLUMNS_WILDCARD\")"
        );
    }
}
