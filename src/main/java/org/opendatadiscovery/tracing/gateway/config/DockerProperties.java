package org.opendatadiscovery.tracing.gateway.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app.docker")
public class DockerProperties {
    private boolean enabled;
    private String host;
    private boolean tlsVerify = false;
    private int namePriority = 10;
}
