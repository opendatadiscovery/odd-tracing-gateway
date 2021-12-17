package org.opendatadiscovery.tracing.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app")
public class AppProperties {
    private String oddrn;
    private int defaultNamePriority = 0;
    private String kafkaServers = "unknown";
    private boolean exposeLatestVersion = true;
}
