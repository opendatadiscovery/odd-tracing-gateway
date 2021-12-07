package org.opendatadiscovery.tracing.gateway.processor;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.opendatadiscovery.tracing.gateway.db.SqlParser;
import org.opendatadiscovery.tracing.gateway.db.SqlStatementInfo;
import org.opendatadiscovery.tracing.gateway.db.TableName;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlParserTest {

    private final SqlParser parser = new SqlParser();

    @TestFactory
    public Iterable<DynamicTest> testSql() {
        return List.of(
            compare(
                "INSERT INTO table_name(column_list) \n"
                    + "VALUES(value_list)\n"
                    + "RETURNING *",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table_name")))
                    .output(Set.of(new TableName("table_name")))
                    .build()
            ),
            compare(
                "SELECT x, y, z FROM schema.table",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("schema", "table")))
                    .build()
            ),
            compare(
                "WITH subquery as (select a from b) SELECT x, y, z FROM table",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table"), new TableName("b")))
                    .build()
            ),
            compare(
                "WITH subquery as (select a from b) SELECT x, y, z FROM table t JOIN subquery s ON s.a=t.x",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table"), new TableName("b")))
                    .build()
            ),
            compare(
                "SELECT x, y, (select a from b) as z FROM table",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table"), new TableName("b")))
                    .build()
            ),
            compare(
                "select \"delete\", \"insertinto\", \"merge\", \"update\" from table",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table")))
                    .build()
            ),
            compare(
                "select col /* from table2 */ from table",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table")))
                    .build()
            ),
            compare(
                "select col from table join anotherTable",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table"), new TableName("anotherTable")))
                    .build()
            ),
            compare(
                "select col from (select * from anotherTable)",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("anotherTable")))
                    .build()
            ),
            compare(
                "select col from (select * from anotherTable) alias",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("anotherTable")))
                    .build()
            ),
            compare(
                "select col from table1 union select col from table2",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table1"), new TableName("table2")))
                    .build()
            ),
            compare(
                "select col from table where col in (select * from anotherTable)",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table"), new TableName("anotherTable")))
                    .build()
            ),
            compare(
                "select col from table1, table2",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table1"), new TableName("table2")))
                    .build()
            ),
            compare(
                "select col from table1 as t1, table2 as t2",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table1"), new TableName("table2")))
                    .build()
            ),
            compare(
                "insert into table VALUES (1, 2)",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table")))
                    .build()
            ),
            compare(
                "delete from table where something",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table")))
                    .build()
            ),
            compare(
                "delete from s12345678",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("s12345678")))
                    .build()
            ),
            compare(
                "update table set answer=42",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table")))
                    .build()
            ),
            compare(
                "update table set answer=(select a from t2 where id=1)",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table")))
                    .input(Set.of(new TableName("t2")))
                    .build()
            ),
            compare(
                "update table set answer=(select a from t2 where id=1) where id > (select a from t3 where id=1)",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table")))
                    .input(Set.of(new TableName("t2"), new TableName("t3")))
                    .build()
            ),
            compare(
                "update table set answer =(select a from t2 where id=1) where id LIKE (select a from t3 where id=1)",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table")))
                    .input(Set.of(new TableName("t2"), new TableName("t3")))
                    .build()
            ),
            compare(
                "WITH subquery AS (SELECT id FROM table) INSERT INTO table1 SELECT * FROM subquery",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table1")))
                    .input(Set.of(new TableName("table")))
                    .build()
            ),
            compare(
                "WITH subquery AS (SELECT id FROM table1), subquery2 AS (SELECT id FROM table2) "
                    + "INSERT INTO table3 SELECT * FROM subquery JOIN subquery2 ON subquery.id=subquery2.id",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table3")))
                    .input(Set.of(new TableName("table1"), new TableName("table2")))
                    .build()
            ),
            compare(
                "WITH subquery AS (SELECT id FROM table1), subquery2 AS (SELECT id FROM table2) "
                    + "INSERT INTO table3 SELECT * FROM subquery",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table3")))
                    .input(Set.of(new TableName("table1"), new TableName("table2")))
                    .build()
            )
        );
    }

    public DynamicTest compare(final String input, final SqlStatementInfo expected) {
        return DynamicTest.dynamicTest(input,
            () -> {
                final SqlStatementInfo parse = parse(input);
                assertEquals(expected, parse);
            }
        );
    }

    public SqlStatementInfo parse(final String statement) {
        return parser.parse(statement);
    }
}
