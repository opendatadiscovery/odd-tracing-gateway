package org.opendatadiscovery.tracing.gateway.config;

import org.opendatadiscovery.oddrn.Generator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {
    @Bean
    public Generator oddrnGenerator() {
        return new Generator();
    }
}
