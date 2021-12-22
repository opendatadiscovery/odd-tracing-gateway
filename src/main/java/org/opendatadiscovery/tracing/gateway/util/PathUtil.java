package org.opendatadiscovery.tracing.gateway.util;

import com.google.common.net.InetAddresses;
import org.apache.commons.lang.math.NumberUtils;

public class PathUtil {

    public static boolean isIp(final String host) {
        return InetAddresses.isInetAddress(host);
    }

    public static String replacePort(final String host) {
        final int pos = host.indexOf(":");
        if (pos > 0) {
            return host.substring(0, pos);
        } else {
            return host;
        }
    }

    public static String sanitize(final String path) {
        final int queryPos = path.indexOf("?");
        final String pathWithoutQuery;
        if (queryPos > 0) {
            pathWithoutQuery = path.substring(0, queryPos);
        } else {
            pathWithoutQuery = path;
        }

        final String[] parts = pathWithoutQuery.split("/");
        if (parts.length == 0) {
            return pathWithoutQuery;
        } else {
            final StringBuilder sb = new StringBuilder();
            for (final String part : parts) {
                if (!part.isEmpty()) {
                    sb.append("/");
                    if (NumberUtils.isNumber(part)) {
                        sb.append("{number}");
                    } else if (isUuid(part)) {
                        sb.append("{uuid}");
                    } else {
                        sb.append(part);
                    }
                }
            }
            return sb.toString();
        }
    }

    public static boolean isUuid(final String name) {
        if (name.length() == 36) {
            final char ch1 = name.charAt(8);
            final char ch2 = name.charAt(13);
            final char ch3 = name.charAt(18);
            final char ch4 = name.charAt(23);
            if (ch1 == '-' && ch2 == '-' && ch3 == '-' && ch4 == '-') {
                return true;
            }
        }
        return false;
    }
}
