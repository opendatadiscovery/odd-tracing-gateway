package org.opendatadiscovery.tracing.gateway.resolver;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.opentelemetry.proto.common.v1.AnyValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.DockerMicroservicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.tracing.gateway.config.K8sProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Component
@ConditionalOnProperty(prefix = "app.k8s", name = "enabled", havingValue = "true")
public class K8sServiceNameResolver implements ServiceNameResolver {
    private final Map<Tuple2<String, String>, String> cache = new ConcurrentHashMap<>();
    private final KubernetesClient client;
    private final List<String> namespaces;
    private final Generator generator;
    private final K8sProperties properties;

    @Autowired
    public K8sServiceNameResolver(final K8sProperties properties, final Generator generator) {
        this.client = new DefaultKubernetesClient();
        this.generator = generator;
        this.properties = properties;

        if (properties.getNamespaces() == null || properties.getNamespaces().isEmpty()) {
            this.namespaces = this.client.namespaces().list().getItems().stream()
                .map(HasMetadata::getFullResourceName)
                .collect(Collectors.toList());
        } else {
            this.namespaces = properties.getNamespaces();
        }
    }

    @Override
    public int priority() {
        return properties.getNamePriority();
    }

    @Override
    public Optional<String> resolve(final Map<String, AnyValue> resourceMap) {
        final Optional<AnyValue> hostName = Optional.ofNullable(resourceMap.get("host.name"));
        final Optional<AnyValue> containerId = Optional.ofNullable(resourceMap.get("container.id"));
        return hostName.flatMap(h -> containerId
            .map(c -> "docker://" + c.getStringValue()).map(c -> Tuples.of(h.getStringValue(), c))
        ).flatMap(this::resolve).map(this::serialize);
    }

    private Optional<OddrnPath> resolve(final Tuple2<String, String> nameAndId) {
        return Optional.ofNullable(
            cache.computeIfAbsent(nameAndId,
                (id) -> resolveClient(nameAndId.getT1(), nameAndId.getT2()).orElse(null)
            )
        ).map(image ->
            DockerMicroservicePath.builder().image(image).build()
        );
    }

    private Optional<String> resolveClient(final String podName, final String containerId) {
        for (final String namespace : this.namespaces) {
            final Pod pod = client.pods().inNamespace(namespace).withName(podName).get();

            if (pod == null) continue;

            final Map<String, ContainerStatus> statuses = pod.getStatus().getContainerStatuses().stream().collect(
                Collectors.toMap(
                    ContainerStatus::getName,
                    s -> s
                )
            );

            for (final Container container : pod.getSpec().getContainers()) {
                final ContainerStatus containerStatus = statuses.get(container.getName());
                if (containerStatus != null && containerStatus.getContainerID().equals(containerId)) {
                    return Optional.of(container.getImage());
                }
            }
        }

        return Optional.empty();
    }

    @SneakyThrows
    private String serialize(final OddrnPath path) {
        return generator.generate(path, "image");
    }
}
