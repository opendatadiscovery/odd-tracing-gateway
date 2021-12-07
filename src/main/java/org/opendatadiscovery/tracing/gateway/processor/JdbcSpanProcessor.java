package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.MysqlPath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.oddrn.model.PostgreSqlPath;
import org.opendatadiscovery.tracing.gateway.db.PostgreSqlParser;
import org.opendatadiscovery.tracing.gateway.db.SqlParser;
import org.opendatadiscovery.tracing.gateway.db.SqlStatementInfo;
import org.opendatadiscovery.tracing.gateway.db.TableName;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.springframework.stereotype.Service;

import static org.opendatadiscovery.tracing.gateway.util.AnyValueUtil.toMap;

@Service
@AllArgsConstructor
public class JdbcSpanProcessor implements SpanProcessor {

    private static final String POSTGRESQL = "postgresql";
    private static final String MYSQL = "mysql";

    private final Generator generator;
    private final SqlParser defaultSqlParser = new SqlParser();
    private final Map<String, SqlParser> sqlParsers = Map.of(
        POSTGRESQL, new PostgreSqlParser()
    );

    @Override
    public List<String> libraries() {
        return List.of("io.opentelemetry.jdbc");
    }

    @Override
    public ServiceOddrns process(final List<Span> spans, final Map<String, AnyValue> keyValue) {
        final Set<String> inputs = new HashSet<>();
        final Set<String> outputs = new HashSet<>();
        for (final Span span : spans) {
            final Map<String, AnyValue> attributes = toMap(span.getAttributesList());

            final String system = Optional.ofNullable(attributes.get("db.system"))
                .map(AnyValue::getStringValue).orElseThrow();

            final Optional<OddrnPath> prefix = prefix(
                system,
                attributes.get("net.peer.name").getStringValue(),
                attributes.get("db.name").getStringValue()
            );

            final AnyValue statement = attributes.get("db.statement");

            if (statement != null && !statement.getStringValue().isEmpty() && prefix.isPresent()) {
                final SqlParser sqlParser = sqlParsers.getOrDefault(system, defaultSqlParser);
                final SqlStatementInfo statementInfo = sqlParser.parse(statement.getStringValue());
                inputs.addAll(
                    statementInfo.getInput().stream()
                        .map(t -> generate(prefix.get(), t))
                        .collect(Collectors.toSet())
                );
                outputs.addAll(
                    statementInfo.getOutput().stream()
                        .map(t -> generate(prefix.get(), t))
                        .collect(Collectors.toSet())
                );
            }
        }
        return ServiceOddrns.builder()
            .inputs(inputs)
            .outputs(outputs)
            .build();
    }

    @SneakyThrows
    private String generate(final OddrnPath prefix, final TableName tableName) {
        final OddrnPath build = build(prefix, tableName);
        return generator.generate(build, "table");
    }

    private OddrnPath build(final OddrnPath prefix, final TableName tableName) {
        if (prefix instanceof PostgreSqlPath) {
            final String schema = Optional.ofNullable(tableName.getSchema()).orElse("public");
            final PostgreSqlPath pgPrefix = (PostgreSqlPath) prefix;
            return pgPrefix.toBuilder()
                .schema(schema)
                .table(tableName.getName())
                .build();
        } else if (prefix instanceof MysqlPath) {
            final MysqlPath mysqlPrefix = (MysqlPath) prefix;
            return mysqlPrefix.toBuilder().table(tableName.getName()).build();
        } else {
            throw new RuntimeException("Unknown path");
        }
    }

    private Optional<OddrnPath> prefix(final String system, final String host, final String database) {
        if (system != null && !system.isEmpty()) {
            return switch (system) {
                case POSTGRESQL -> Optional.of(PostgreSqlPath.builder()
                    .host(host)
                    .database(database)
                    .build()
                );
                case MYSQL -> Optional.of(MysqlPath.builder()
                    .host(host)
                    .database(database)
                    .build()
                );
                default -> Optional.empty();
            };
        } else {
            return Optional.empty();
        }
    }
}
