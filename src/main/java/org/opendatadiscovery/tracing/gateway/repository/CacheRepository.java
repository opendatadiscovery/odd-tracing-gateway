package org.opendatadiscovery.tracing.gateway.repository;

import java.time.OffsetDateTime;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface CacheRepository {
    Mono<ServiceOddrns> get(String versionedOddrn);

    Flux<ServiceOddrns> getServices(OffsetDateTime changedSince);

    Mono<ServiceOddrns> add(ServiceOddrns oddrns);
}
