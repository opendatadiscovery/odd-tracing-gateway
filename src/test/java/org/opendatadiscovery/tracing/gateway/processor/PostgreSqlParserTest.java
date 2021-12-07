package org.opendatadiscovery.tracing.gateway.processor;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.opendatadiscovery.tracing.gateway.db.PostgreSqlParser;
import org.opendatadiscovery.tracing.gateway.db.SqlStatementInfo;
import org.opendatadiscovery.tracing.gateway.db.TableName;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgreSqlParserTest extends SqlParserTest {
    private final PostgreSqlParser parser = new PostgreSqlParser();

    @TestFactory
    public Iterable<DynamicTest> testPostgreSqlSpecifics() {
        return List.of(
            compare(
                "INSERT INTO table_name(column_list) \n"
                    + "VALUES(value_list)\n"
                    + "ON CONFLICT target action",
                SqlStatementInfo.builder()
                    .output(Set.of(new TableName("table_name")))
                    .build()
            ),
            compare(
                "INSERT INTO table_name(column_list) \n"
                    + "VALUES(value_list)\n"
                    + "ON CONFLICT DO UPDATE SET column_list=value_list RETURNING *",
                SqlStatementInfo.builder()
                    .input(Set.of(new TableName("table_name")))
                    .output(Set.of(new TableName("table_name")))
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
        final String removed = parser.applyHooks(
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

    @Test
    public void testKeepReturning() {
        final String removed = parser.applyHooks(
            "INSERT INTO table_name(column_list) \n"
                + "VALUES(value_list)\n"
                + "RETURNING table_name.*"
        );
        assertEquals(
            "INSERT INTO table_name(column_list) \n"
                + "VALUES(value_list)\n"
                + "RETURNING table_name.*",
            removed
        );

        final String removed2 = parser.applyHooks(
            "INSERT INTO table_name(column_list) \n"
                + "VALUES(value_list)\n"
                + "ON CONFLICT DO UPDATE SET column_list=value_list RETURNING table_name.*"
        );
        assertEquals(
            "INSERT INTO table_name(column_list) \n"
                + "VALUES(value_list)\n"
                + "RETURNING table_name.*",
            removed2
        );
    }

    @Override
    public SqlStatementInfo parse(final String statement) {
        return parser.parse(statement);
    }
}
