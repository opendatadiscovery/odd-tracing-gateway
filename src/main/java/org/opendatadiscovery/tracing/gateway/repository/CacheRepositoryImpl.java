package org.opendatadiscovery.tracing.gateway.repository;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.springframework.data.redis.core.ReactiveHashOperations;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveSetOperations;
import org.springframework.data.redis.core.ReactiveValueOperations;
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
    private static final Object NAME_KEY = "name";

    private final ReactiveRedisTemplate<String, String> redisTemplate;

    public Mono<ServiceOddrns> get(final String oddrn) {
        final ReactiveSetOperations<String, String> ops = redisTemplate.opsForSet();
        final ReactiveHashOperations<String, String, String> opsHash = redisTemplate.opsForHash();
        final ReactiveValueOperations<String, String> valueOps = redisTemplate.opsForValue();

        final String oddrnKey = String.format("%s-%s", PREFIX, oddrn);
        final String oddrnKeyName = String.format("%s-%s", oddrnKey, NAME_KEY);
        final String inputKey = String.format("%s-%s", oddrnKey, INPUT_KEY);
        final String outputKey = String.format("%s-%s", oddrnKey, OUTPUT_KEY);

        return ops.members(inputKey)
            .collectList()
            .zipWith(
                ops.members(outputKey).collectList(),
                (l1, l2) -> ServiceOddrns.builder()
                    .oddrn(oddrn)
                    .inputs(new HashSet<>(l1))
                    .outputs(new HashSet<>(l2))
                    .build()
            ).zipWith(
                opsHash.entries(oddrnKey).collectList()
                    .map(l -> l.stream().collect(
                            Collectors.toMap(
                                Map.Entry::getKey,
                                Map.Entry::getValue
                            )
                        )
                    ),
                (s, m) -> s.toBuilder().metadata(m).build()
            ).zipWith(
                valueOps.get(oddrnKeyName),
                (s, n) -> s.toBuilder().name(n).build()
            );
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
        final ReactiveValueOperations<String, String> valueOps = redisTemplate.opsForValue();

        final String oddrnKey = String.format("%s-%s", PREFIX, oddrns.getOddrn());
        final String oddrnKeyName = String.format("%s-%s", oddrnKey, NAME_KEY);
        final String inputKey = String.format("%s-%s", oddrnKey, INPUT_KEY);
        final String outputKey = String.format("%s-%s", oddrnKey, OUTPUT_KEY);

        final Mono<Boolean> metadataMono = hashOps.putAll(oddrnKey, oddrns.getMetadata());

        final Mono<Boolean> nameMono = valueOps.set(oddrnKeyName, oddrns.getName());

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
                    ? updateService(oddrns.getOddrn()).map(res -> oddrns)
                    : Mono.just(oddrns);
            }
        ).flatMap(o -> {
            log.info("oddrns updated {}", o);
            return metadataMono.flatMap(m -> nameMono).map(r -> o);
        });
    }

    private Mono<Boolean> updateService(final String service) {
        final ReactiveHashOperations<String, String, String> ops = redisTemplate.opsForHash();
        return ops.put(UPDATES_KEY, service, Long.valueOf(Instant.now().toEpochMilli()).toString());
    }
}
