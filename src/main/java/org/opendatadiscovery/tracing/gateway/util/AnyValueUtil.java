package org.opendatadiscovery.tracing.gateway.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AnyValueUtil {
    public static ObjectMapper mapper;

    public static boolean isSetString(final AnyValue anyValue) {
        return anyValue != null && !anyValue.getStringValue().isEmpty();
    }

    public static Optional<String> getString(final Map<String, AnyValue> keyValueMap, final String key) {
        return Optional.ofNullable(keyValueMap.get(key)).map(AnyValue::getStringValue);
    }

    public static Map<String, AnyValue> toMap(final List<KeyValue> list) {
        return list.stream().collect(
            Collectors.toMap(
                KeyValue::getKey,
                KeyValue::getValue
            )
        );
    }

    public static Map<String, String> toStringMap(final Map<String, AnyValue> map, final Map<String, AnyValue> addon) {
        return Stream.concat(
            map.entrySet().stream(),
            addon.entrySet().stream()
        ).collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e -> toString(e.getValue())
            )
        );
    }

    public static String toString(final AnyValue value) {
        return switch (value.getValueCase()) {
            case STRING_VALUE -> value.getStringValue();
            case BOOL_VALUE -> Boolean.valueOf(value.getBoolValue()).toString();
            case INT_VALUE -> Long.valueOf(value.getIntValue()).toString();
            case ARRAY_VALUE -> value.getArrayValue().getValuesList()
                .stream().map(AnyValueUtil::toString).collect(Collectors.joining(","));
            case KVLIST_VALUE -> toJsonString(value.getKvlistValue());
            default -> "";
        };
    }

    public static String toJsonString(final KeyValueList kvList) {
        final ObjectNode objectNode = mapper.createObjectNode();

        for (final KeyValue keyValue : kvList.getValuesList()) {
            objectNode.put(keyValue.getKey(), toString(keyValue.getValue()));
        }

        return objectNode.toPrettyString();
    }
}
