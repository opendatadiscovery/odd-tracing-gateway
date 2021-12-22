package org.opendatadiscovery.tracing.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app.http")
public class HttpProperties {
    private String staticPrefix = "/static/";
    private boolean excludeIps = true;
}
