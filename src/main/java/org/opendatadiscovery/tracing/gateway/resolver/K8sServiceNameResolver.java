package org.opendatadiscovery.tracing.gateway.resolver;

import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerStatus;
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
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.DockerMicroservicePath;
import org.opendatadiscovery.tracing.gateway.config.K8sProperties;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.opendatadiscovery.tracing.gateway.util.VersionUtil.parseName;

@Component
@ConditionalOnProperty(prefix = "app.k8s", name = "enabled", havingValue = "true")
@Slf4j
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
                .map(n -> n.getMetadata().getName())
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
    public Optional<NameOddrn> resolve(final Map<String, AnyValue> resourceMap) {
        final Optional<AnyValue> hostName = Optional.ofNullable(resourceMap.get("host.name"));
        final Optional<AnyValue> containerId = Optional.ofNullable(resourceMap.get("container.id"));
        return hostName.flatMap(h -> containerId
            .map(c -> "docker://" + c.getStringValue()).map(c -> Tuples.of(h.getStringValue(), c))
        ).flatMap(this::resolve).map(this::serialize);
    }

    private Optional<String> resolve(final Tuple2<String, String> nameAndId) {
        return Optional.ofNullable(
            cache.computeIfAbsent(nameAndId,
                (id) -> {
                    final Optional<String> image = resolveClient(nameAndId.getT1(), nameAndId.getT2());
                    if (image.isEmpty()) {
                        log.error("Pod {} with container id {} not found", nameAndId.getT1(), nameAndId.getT2());
                        return null;
                    } else {
                        return image.get();
                    }
                }
            )
        );
    }

    private Optional<String> resolveClient(final String podName, final String containerId) {
        log.info("namespaces: {}", this.namespaces);
        for (final String namespace : this.namespaces) {
            final Pod pod = client.pods().inNamespace(namespace).withName(podName).get();

            if (pod == null) {
                continue;
            }

            log.info("found pod {} in namespace: {}", podName, namespace);

            final Map<String, ContainerStatus> statuses = pod.getStatus().getContainerStatuses().stream().collect(
                Collectors.toMap(
                    ContainerStatus::getName,
                    s -> s
                )
            );

            log.info("container statuses: {}", statuses);

            for (final Container container : pod.getSpec().getContainers()) {
                final ContainerStatus containerStatus = statuses.get(container.getName());
                if (containerStatus != null && containerStatus.getContainerID().equals(containerId)) {
                    log.info("image name: {}", container.getImage());
                    return Optional.of(container.getImage());
                }
            }
        }

        log.info("container id {} not found", containerId);
        return Optional.empty();
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
