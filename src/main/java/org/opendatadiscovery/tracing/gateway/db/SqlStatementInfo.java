package org.opendatadiscovery.tracing.gateway.db;

import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class SqlStatementInfo {
    @Builder.Default
    private final Set<TableName> input = new HashSet<>();

    @Builder.Default
    private final Set<TableName> output = new HashSet<>();
}
