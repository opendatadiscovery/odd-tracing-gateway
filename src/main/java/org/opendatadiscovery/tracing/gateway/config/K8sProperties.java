package org.opendatadiscovery.tracing.gateway.config;

import java.util.List;
import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "app.k8s")
public class K8sProperties {
    private boolean enabled;
    private String host;
    private List<String> namespaces = List.of();
    private int namePriority = 10;
}
