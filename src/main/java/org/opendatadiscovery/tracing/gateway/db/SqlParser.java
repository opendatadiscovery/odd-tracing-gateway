package org.opendatadiscovery.tracing.gateway.db;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import reactor.util.function.Tuple2;

public class SqlParser {

    @SneakyThrows
    public SqlStatementInfo parse(final String statement) {
        final Statement stmt = CCJSqlParserUtil.parse(
            applyHooks(statement)
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

    public String applyHooks(final String statement) {
        String result = statement;
        for (final Tuple2<Pattern, String> entry : hooks()) {
            result = entry.getT1().matcher(result).replaceAll(entry.getT2());
        }
        return result;
    }

    public List<Tuple2<Pattern, String>> hooks() {
        return List.of();
    }
}
