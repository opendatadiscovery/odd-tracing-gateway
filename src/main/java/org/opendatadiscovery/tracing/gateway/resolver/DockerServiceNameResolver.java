package org.opendatadiscovery.tracing.gateway.resolver;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.transport.DockerHttpClient;
import com.github.dockerjava.zerodep.ZerodepDockerHttpClient;
import io.opentelemetry.proto.common.v1.AnyValue;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.DockerMicroservicePath;
import org.opendatadiscovery.tracing.gateway.config.DockerProperties;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import static org.opendatadiscovery.tracing.gateway.util.VersionUtil.parseName;

@Component
@Slf4j
@ConditionalOnProperty(prefix = "app.docker", name = "enabled", havingValue = "true")
public class DockerServiceNameResolver implements ServiceNameResolver {
    private final Map<String, String> cache = new ConcurrentHashMap<>();
    private final DockerProperties properties;
    private final DockerClient client;
    private final Generator generator;

    @Autowired
    public DockerServiceNameResolver(final DockerProperties properties, final Generator generator) {
        this.properties = properties;
        this.generator = generator;

        final DockerClientConfig config = DefaultDockerClientConfig.createDefaultConfigBuilder()
            .withDockerHost(properties.getHost())
            .withDockerTlsVerify(properties.isTlsVerify())
            .build();

        final DockerHttpClient httpClient = new ZerodepDockerHttpClient.Builder()
            .dockerHost(config.getDockerHost())
            .sslConfig(config.getSSLConfig())
            .maxConnections(100)
            .connectionTimeout(Duration.ofSeconds(30))
            .responseTimeout(Duration.ofSeconds(45))
            .build();

        this.client = DockerClientImpl.getInstance(config, httpClient);
    }

    @Override
    public int priority() {
        return properties.getNamePriority();
    }

    @Override
    public Optional<NameOddrn> resolve(final Map<String, AnyValue> resourceMap) {
        final Optional<NameOddrn> resolved =
            Optional.ofNullable(resourceMap.get("container.id")).flatMap(id -> resolve(id.getStringValue()))
                .map(this::serialize);
        log.info("Resolved: {}", resolved);
        return resolved;
    }

    private Optional<String> resolve(final String containerId) {
        return Optional.ofNullable(
            cache.computeIfAbsent(containerId, (id) -> resolveClient(id).orElse(null))
        );
    }

    private Optional<String> resolveClient(final String containerId) {
        final InspectContainerResponse response = client.inspectContainerCmd(containerId).exec();
        return Optional.ofNullable(response.getConfig().getImage());
    }

    @SneakyThrows
    private NameOddrn serialize(final String image) {
        final NameOddrn name = parseName(
            image
        );
        return name.toBuilder()
            .oddrn(
                generator.generate(
                    DockerMicroservicePath.builder().image(name.getName()).build(),
                    "image"
                )
            )
            .build();
    }
}
