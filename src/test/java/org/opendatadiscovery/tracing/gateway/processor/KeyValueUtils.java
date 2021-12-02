package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;

public class KeyValueUtils {
    public static KeyValue withString(final String key, final String value) {
        return KeyValue.newBuilder()
            .setKey(key).setValue(AnyValue.newBuilder().setStringValue(value).build())
            .build();
    }

    public static KeyValue withInt(final String key, final Integer value) {
        return KeyValue.newBuilder()
            .setKey(key).setValue(AnyValue.newBuilder().setIntValue(value).build())
            .build();
    }
}
