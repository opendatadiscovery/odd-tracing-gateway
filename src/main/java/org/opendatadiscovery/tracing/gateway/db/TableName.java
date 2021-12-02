package org.opendatadiscovery.tracing.gateway.db;

import lombok.Data;

@Data
public class TableName {

    private final String schema;
    private final String name;

    public TableName(final String name) {
        this(null, name);
    }

    public TableName(final String schema, final String name) {
        this.schema = strip(schema);
        this.name = strip(name);
    }

    private String strip(final String value) {
        if (value != null && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        } else {
            return value;
        }
    }
}
