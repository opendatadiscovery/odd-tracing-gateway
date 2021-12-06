package org.opendatadiscovery.tracing.gateway.processor;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.opendatadiscovery.tracing.gateway.db.SqlParser;
import org.opendatadiscovery.tracing.gateway.db.SqlStatementInfo;
import org.opendatadiscovery.tracing.gateway.db.TableName;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlParserTest {

    @TestFactory
    public Iterable<DynamicTest> testSql() {
        return List.of(
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
            ),
            compare(
                "WITH subquery AS MATERIALIZED (SELECT id FROM table1), subquery2 AS (SELECT id FROM table2) "
                    + "INSERT INTO table3 SELECT * FROM subquery",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table3")))
                    .input(Set.of(new TableName("table1"), new TableName("table2")))
                    .build()
            ),
            compare(
                "WITH subquery AS MATERIALIZED (SELECT id FROM table1)"
                    + ", subquery2 AS MATERIALIZED (SELECT id FROM table2) "
                    + "SELECT json_agg(subquery.*) as name FROM subquery",
                SqlStatementInfo.builder()
                    .output(Set.of())
                    .input(Set.of(new TableName("table1"), new TableName("table2")))
                    .build()
            ),
            compare(
                "with \"dataEntityCTE\" as materialized (\n"
                    + "  select \n"
                    + "    * \n"
                    + "  from \n"
                    + "    \"public\".\"data_entity\" \n"
                    + "  where \n"
                    + "    \"public\".\"data_entity\".\"hollow\" = false\n"
                    + ") \n"
                    + "select \n"
                    + "  \"dataEntityCTE\".\"id\", \n"
                    + "  \"dataEntityCTE\".\"internal_name\", \n"
                    + "  \"dataEntityCTE\".\"external_name\", \n"
                    + "  \"dataEntityCTE\".\"oddrn\", \n"
                    + "  \"dataEntityCTE\".\"data_source_id\", \n"
                    + "  \"dataEntityCTE\".\"created_at\", \n"
                    + "  \"dataEntityCTE\".\"updated_at\", \n"
                    + "  \"dataEntityCTE\".\"subtype_id\", \n"
                    + "  \"dataEntityCTE\".\"specific_attributes\", \n"
                    + "  \"dataEntityCTE\".\"external_description\", \n"
                    + "  \"dataEntityCTE\".\"internal_description\", \n"
                    + "  \"dataEntityCTE\".\"hollow\", \n"
                    + "  \"dataEntityCTE\".\"view_count\", \n"
                    + "  \"public\".\"namespace\".\"id\", \n"
                    + "  \"public\".\"namespace\".\"name\", \n"
                    + "  \"public\".\"namespace\".\"is_deleted\", \n"
                    + "  \"public\".\"namespace\".\"created_at\", \n"
                    + "  \"public\".\"namespace\".\"updated_at\", \n"
                    + "  \"public\".\"data_source\".\"id\", \n"
                    + "  \"public\".\"data_source\".\"name\", \n"
                    + "  \"public\".\"data_source\".\"oddrn\", \n"
                    + "  \"public\".\"data_source\".\"description\", \n"
                    + "  \"public\".\"data_source\".\"active\", \n"
                    + "  \"public\".\"data_source\".\"connection_url\", \n"
                    + "  \"public\".\"data_source\".\"is_deleted\", \n"
                    + "  \"public\".\"data_source\".\"created_at\", \n"
                    + "  \"public\".\"data_source\".\"updated_at\", \n"
                    + "  \"public\".\"data_source\".\"pulling_interval\", \n"
                    + "  \"public\".\"data_source\".\"namespace_id\", \n"
                    + "  \"public\".\"data_entity_subtype\".\"id\", \n"
                    + "  \"public\".\"data_entity_subtype\".\"name\", \n"
                    + "  json_agg(\"public\".\"data_entity_type\".*) as \"type\", \n"
                    + "  json_agg(\"public\".\"tag\".*) as \"tag\", \n"
                    + "  json_agg(\"public\".\"owner\".*) as \"owner\", \n"
                    + "  json_agg(\"public\".\"role\".*) as \"role\", \n"
                    + "  json_agg(\"public\".\"ownership\".*) as \"ownership\", \n"
                    + "  json_agg(\"public\".\"alert\".*) as \"alert\" \n"
                    + "from \n"
                    + "  \"dataEntityCTE\" \n"
                    + "  left outer join \"public\".\"type_entity_relation\" on "
                    + " \"dataEntityCTE\".\"id\" = \"public\".\"type_entity_relation\".\"data_entity_id\" \n"
                    + "  left outer join \"public\".\"data_entity_type\" on "
                    + "\"public\".\"type_entity_relation\".\"data_entity_type_id\" = "
                    + "\"public\".\"data_entity_type\".\"id\" \n"
                    + "  left outer join \"public\".\"data_entity_subtype\" on "
                    + "\"dataEntityCTE\".\"subtype_id\" = \"public\".\"data_entity_subtype\".\"id\" \n"
                    + "  left outer join \"public\".\"tag_to_data_entity\" on "
                    + "\"dataEntityCTE\".\"id\" = \"public\".\"tag_to_data_entity\".\"data_entity_id\" \n"
                    + "  left outer join \"public\".\"tag\" on "
                    + "\"public\".\"tag_to_data_entity\".\"tag_id\" = \"public\".\"tag\".\"id\" \n"
                    + "  left outer join \"public\".\"data_source\" on "
                    + "\"dataEntityCTE\".\"data_source_id\" = \"public\".\"data_source\".\"id\" \n"
                    + "  left outer join \"public\".\"namespace\" on "
                    + "\"public\".\"data_source\".\"namespace_id\" = \"public\".\"namespace\".\"id\" \n"
                    + "  left outer join \"public\".\"ownership\" on "
                    + "\"dataEntityCTE\".\"id\" = \"public\".\"ownership\".\"data_entity_id\" \n"
                    + "  left outer join \"public\".\"owner\" on "
                    + "\"public\".\"ownership\".\"owner_id\" = \"public\".\"owner\".\"id\" \n"
                    + "  left outer join \"public\".\"role\" on "
                    + "\"public\".\"ownership\".\"role_id\" = \"public\".\"role\".\"id\" \n"
                    + "  left outer join \"public\".\"alert\" on "
                    + "\"public\".\"alert\".\"data_entity_oddrn\" = \"dataEntityCTE\".\"oddrn\" \n"
                    + "group by \n"
                    + "  \"dataEntityCTE\".\"id\", \n"
                    + "  \"dataEntityCTE\".\"internal_name\", \n"
                    + "  \"dataEntityCTE\".\"external_name\", \n"
                    + "  \"dataEntityCTE\".\"oddrn\", \n"
                    + "  \"dataEntityCTE\".\"data_source_id\", \n"
                    + "  \"dataEntityCTE\".\"created_at\", \n"
                    + "  \"dataEntityCTE\".\"updated_at\", \n"
                    + "  \"dataEntityCTE\".\"subtype_id\", \n"
                    + "  \"dataEntityCTE\".\"specific_attributes\", \n"
                    + "  \"dataEntityCTE\".\"external_description\", \n"
                    + "  \"dataEntityCTE\".\"internal_description\", \n"
                    + "  \"dataEntityCTE\".\"hollow\", \n"
                    + "  \"dataEntityCTE\".\"view_count\", \n"
                    + "  \"public\".\"namespace\".\"id\", \n"
                    + "  \"public\".\"namespace\".\"name\", \n"
                    + "  \"public\".\"namespace\".\"is_deleted\", \n"
                    + "  \"public\".\"namespace\".\"created_at\", \n"
                    + "  \"public\".\"namespace\".\"updated_at\", \n"
                    + "  \"public\".\"data_source\".\"id\", \n"
                    + "  \"public\".\"data_source\".\"name\", \n"
                    + "  \"public\".\"data_source\".\"oddrn\", \n"
                    + "  \"public\".\"data_source\".\"description\", \n"
                    + "  \"public\".\"data_source\".\"active\", \n"
                    + "  \"public\".\"data_source\".\"connection_url\", \n"
                    + "  \"public\".\"data_source\".\"is_deleted\", \n"
                    + "  \"public\".\"data_source\".\"created_at\", \n"
                    + "  \"public\".\"data_source\".\"updated_at\", \n"
                    + "  \"public\".\"data_source\".\"pulling_interval\", \n"
                    + "  \"public\".\"data_source\".\"namespace_id\", \n"
                    + "  \"public\".\"data_entity_subtype\".\"id\", \n"
                    + "  \"public\".\"data_entity_subtype\".\"name\" \n"
                    + "limit \n"
                    + "  30 offset 0\n",
                SqlStatementInfo.builder()
                    .output(Set.of())
                    .input(Set.of(
                        new TableName("public", "data_entity"),
                        new TableName("public", "data_entity_subtype"),
                        new TableName("public", "data_entity_type"),
                        new TableName("public", "owner"),
                        new TableName("public", "tag_to_data_entity"),
                        new TableName("public", "type_entity_relation"),
                        new TableName("public", "ownership"),
                        new TableName("public", "alert"),
                        new TableName("public", "data_source"),
                        new TableName("public", "namespace"),
                        new TableName("public", "tag"),
                        new TableName("public", "role")
                    ))
                    .build()
            )
        );
    }

    @Test
    public void testRemoveMaterialized() {
        final String removed = SqlParser.removeMaterialized(
            "with \"dataEntityCTE\" as materialized (\n"
                + "  select \n"
                + "    *, \n"
                + "  from \n"
                + "    \"public\".\"data_entity\" \n"
                + "  where \n"
                + "    \"public\".\"data_entity\".\"hollow\" = false\n"
                + ") \n"
        );
        assertEquals(
            "with \"dataEntityCTE\" AS (\n"
                + "  select \n"
                + "    *, \n"
                + "  from \n"
                + "    \"public\".\"data_entity\" \n"
                + "  where \n"
                + "    \"public\".\"data_entity\".\"hollow\" = false\n"
                + ") \n",
            removed
        );
    }

    public DynamicTest compare(final String input, final SqlStatementInfo expected) {
        return DynamicTest.dynamicTest(input,
            () -> {
                final SqlStatementInfo parse = SqlParser.parse(input);
                assertEquals(expected, parse);
            }
        );
    }
}
