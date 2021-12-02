package org.opendatadiscovery.tracing.gateway.db;

import java.util.stream.Collectors;
import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

public class SqlParser {
    @SneakyThrows
    public static SqlStatementInfo parse(final String statement) {
        final Statement stmt = CCJSqlParserUtil.parse(statement);
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
}
