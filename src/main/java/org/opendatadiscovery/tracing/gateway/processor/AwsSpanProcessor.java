package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.trace.v1.Span;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.exception.EmptyPathValueException;
import org.opendatadiscovery.oddrn.exception.PathDoesntExistException;
import org.opendatadiscovery.oddrn.model.DynamodbPath;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.opendatadiscovery.tracing.gateway.util.AnyValueUtil;
import org.springframework.stereotype.Service;

import static org.opendatadiscovery.tracing.gateway.util.AnyValueUtil.isSetString;

@Service
@AllArgsConstructor
@Slf4j
public class AwsSpanProcessor implements SpanProcessor {
    private final Generator generator;

    @Override
    public boolean accept(final String library) {
        return library.startsWith("io.opentelemetry.aws-sdk");
    }

    @Override
    public List<ServiceOddrns> process(final List<Span> spans,
                                       final Map<String, AnyValue> keyValue,
                                       final NameOddrn service) {
        final Set<String> inputs = new HashSet<>();
        final Set<String> outputs = new HashSet<>();

        for (final Span span : spans) {
            if (span.getName().startsWith("DynamoDb")) {
                try {
                    dynamoDb(span, inputs, outputs);
                } catch (Throwable e) {
                    log.error("Error processing span", e);
                }
            }
        }

        return List.of(
            ServiceOddrns.builder()
                .inputs(inputs)
                .outputs(outputs)
                .oddrn(service.getOddrn())
                .name(service.getName())
                .version(service.getVersion())
                .metadata(AnyValueUtil.toStringMap(keyValue))
                .build()
        );
    }

    private void dynamoDb(final Span span, final Set<String> inputs, final Set<String> outputs)
            throws PathDoesntExistException, EmptyPathValueException, InvocationTargetException,
            IllegalAccessException {
        final Map<String, AnyValue> attributes = span.getAttributesList().stream().collect(
            Collectors.toMap(
                KeyValue::getKey,
                KeyValue::getValue
            )
        );

        final AnyValue operation = attributes.get("db.operation");
        final AnyValue tableName = attributes.get("aws.table.name");
        final AnyValue tableNames = attributes.get("aws.dynamodb.table_names");
        final AnyValue httpUrl = attributes.get("http.url");

        final DynamodbPath prefix = DynamodbPath.builder()
            .account("unknown") // TODO: find
            .region("unknown")
            .build();

        if (isSetString(operation) && isSetString(tableName) && isSetString(httpUrl)) {
            final DynamodbPath path = prefix.toBuilder().table(tableName.getStringValue()).build();

            switch (operation.getStringValue()) {
                case "GetItem":
                    inputs.add(generator.generate(path, "table"));
                    break;
                case "PutItem":
                case "DeleteItem":
                case "UpdateItem":
                    outputs.add(generator.generate(path, "table"));
                    break;
                default:
                    break;
            }
        } else if (isSetString(operation) && isSetString(tableNames) && isSetString(httpUrl)) {
            final String operatoinValue = operation.getStringValue();
            if ("BatchGetItem".equals(operatoinValue)) {
                for (final String t : tableNames.getStringValue().split(",")) {
                    inputs.add(generator.generate(prefix.toBuilder().table(t).build(), "table"));
                }
            } else if ("BatchWriteItem".equals(operatoinValue)) {
                for (final String t : tableNames.getStringValue().split(",")) {
                    outputs.add(generator.generate(prefix.toBuilder().table(t).build(), "table"));
                }
            }
        }
    }
}
