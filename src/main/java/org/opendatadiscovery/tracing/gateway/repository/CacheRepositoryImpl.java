package org.opendatadiscovery.tracing.gateway.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.adapter.contract.model.DataEntityType;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
@RequiredArgsConstructor
@Slf4j
public class CacheRepositoryImpl implements CacheRepository {
    private static final String PREFIX = "odd";
    private static final String UPDATES_KEY = String.format("%s-updates", PREFIX);
    private static final String INPUT_KEY = "input";
    private static final String OUTPUT_KEY = "output";
    private static final String NAME_KEY = "SYSTEM_ODD_NAME";
    private static final String TYPE_KEY = "SYSTEM_ODD_TYPE";
    private static final String VERSION_KEY = "SYSTEM_ODD_VERSION";
    private static final String VERSION_SPLITTER = ":::";

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public Mono<ServiceOddrns> get(final String versionedOddrn) {
        final ReactiveSetOperations<String, String> ops = redisTemplate.opsForSet();
        final ReactiveHashOperations<String, String, String> opsHash = redisTemplate.opsForHash();

        final String oddrnKey = String.format("%s-%s", PREFIX, versionedOddrn);
        final String inputKey = String.format("%s-%s", oddrnKey, INPUT_KEY);
        final String outputKey = String.format("%s-%s", oddrnKey, OUTPUT_KEY);

        final String[] versionedParts = versionedOddrn.split(VERSION_SPLITTER);
        if (versionedParts.length < 2) {
            // To skip old version updates
            return Mono.empty();
        }
        final String oddrn = versionedParts[0];
        final String version = versionedParts[1];

        return ops.members(inputKey)
            .collectList()
            .zipWith(
                ops.members(outputKey).collectList(),
                (l1, l2) -> ServiceOddrns.builder()
                    .oddrn(oddrn)
                    .version(version)
                    .inputs(new HashSet<>(l1))
                    .outputs(new HashSet<>(l2))
                    .build()
            ).zipWith(
                opsHash.entries(oddrnKey).collectList(),
                this::parseMetadata
            );
    }

    private ServiceOddrns parseMetadata(final ServiceOddrns oddrns, final List<Map.Entry<String, String>> entries) {
        final Map<String, String> metadata = new HashMap<>();
        String name = "unknown";
        DataEntityType type = DataEntityType.MICROSERVICE;
        String version = "unknown";

        for (final Map.Entry<String, String> entry : entries) {
            switch (entry.getKey()) {
                case NAME_KEY -> name = entry.getValue();
                case TYPE_KEY -> type = DataEntityType.fromValue(entry.getValue());
                case VERSION_KEY -> version = entry.getValue();
                default -> metadata.put(entry.getKey(), entry.getValue());
            }
        }

        return oddrns.toBuilder()
            .name(name)
            .serviceType(type)
            .version(version)
            .metadata(metadata)
            .build();
    }

    @Override
    public Flux<ServiceOddrns> getServices(final OffsetDateTime changedSince) {
        final ReactiveHashOperations<String, String, String> ops = redisTemplate.opsForHash();
        final long epochMillis = Optional.ofNullable(changedSince).map(s -> s.toInstant().toEpochMilli()).orElse(0L);
        return ops.entries(UPDATES_KEY)
            .filter(e -> Long.parseLong(e.getValue()) >= epochMillis)
            .flatMap(e ->
                get(e.getKey())
                    .map(o -> o.toBuilder().updatedAt(Instant.ofEpochMilli(Long.parseLong(e.getValue()))).build())
            );
    }

    public Mono<ServiceOddrns> add(final ServiceOddrns oddrns) {
        final ReactiveSetOperations<String, String> ops = redisTemplate.opsForSet();
        final ReactiveHashOperations<String, String, String> hashOps = redisTemplate.opsForHash();

        final String oddrnKey = String.format("%s-%s%s%s",
            PREFIX, oddrns.getOddrn(), VERSION_SPLITTER, oddrns.getVersion());
        final String inputKey = String.format("%s-%s", oddrnKey, INPUT_KEY);
        final String outputKey = String.format("%s-%s", oddrnKey, OUTPUT_KEY);

        final Map<String, String> metadata = new HashMap<>(oddrns.getMetadata());
        metadata.put(NAME_KEY, oddrns.getName());
        metadata.put(TYPE_KEY, oddrns.getServiceType().getValue());
        metadata.put(VERSION_KEY, oddrns.getVersion());

        final Mono<Boolean> metadataMono = hashOps.putAll(oddrnKey, metadata);

        final Mono<Boolean> groupMono =
            oddrns.getGroupOddrn() != null && oddrns.getGroupType() != null
                ? this.add(
                    ServiceOddrns.builder()
                        .name(oddrns.getGroupName())
                        .serviceType(oddrns.getGroupType())
                        .oddrn(oddrns.getGroupOddrn())
                        .inputs(Set.of(oddrns.getOddrn()))
                        .outputs(Set.of())
                        .version(oddrns.getVersion())
                        .build()
                ).map(s -> true)
                : Mono.just(false);

        final Mono<Long> inputs = oddrns.getInputs().size() > 0
            ? ops.add(inputKey, oddrns.getInputs().toArray(new String[0]))
            : Mono.just(0L);

        final Mono<Long> outputs = oddrns.getOutputs().size() > 0
            ? ops.add(outputKey, oddrns.getOutputs().toArray(new String[0]))
            : Mono.just(0L);

        return Flux.merge(
            inputs, outputs
        ).collectList().flatMap(r -> {
                log.info("results: {}", r);
                return r.stream().anyMatch(l -> l > 0)
                    ? updateService(oddrns.getOddrn(), oddrns.getVersion()).map(res -> oddrns)
                    : Mono.just(oddrns);
            }
        ).flatMap(o -> {
            log.info("oddrns updated {}", o);
            return metadataMono
                .flatMap(m -> groupMono)
                .map(r -> o);
        });
    }

    private Mono<Boolean> updateService(final String service, final String version) {
        final ReactiveHashOperations<String, String, String> ops = redisTemplate.opsForHash();
        final String versionedService = String.format("%s%s%s", service, VERSION_SPLITTER, version);
        return ops.put(UPDATES_KEY, versionedService, Long.valueOf(Instant.now().toEpochMilli()).toString());
    }
}
