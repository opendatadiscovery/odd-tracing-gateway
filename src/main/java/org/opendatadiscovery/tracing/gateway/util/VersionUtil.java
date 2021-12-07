package org.opendatadiscovery.tracing.gateway.util;

import org.opendatadiscovery.tracing.gateway.model.NameOddrn;

public class VersionUtil {
    public static NameOddrn parseName(final String name) {
        final int pos = name.indexOf(":");
        if (pos > 0) {
            return NameOddrn.builder()
                .name(name.substring(0, pos))
                .version(name.substring(pos))
                .build();
        } else {
            return NameOddrn.builder()
                .name(name)
                .version("unknown")
                .build();
        }
    }
}
