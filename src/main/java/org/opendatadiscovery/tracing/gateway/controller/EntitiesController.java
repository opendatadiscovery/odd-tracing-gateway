package org.opendatadiscovery.tracing.gateway.controller;

import java.time.OffsetDateTime;
import javax.validation.Valid;
import lombok.AllArgsConstructor;
import org.opendatadiscovery.adapter.contract.api.EntitiesApi;
import org.opendatadiscovery.adapter.contract.model.DataEntityList;
import org.opendatadiscovery.tracing.gateway.mapper.ServiceMapper;
import org.opendatadiscovery.tracing.gateway.repository.CacheRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

@AllArgsConstructor
@RestController
public class EntitiesController implements EntitiesApi {
    private final CacheRepository cacheRepository;
    private final ServiceMapper mapper;

    @Override
    public Mono<ResponseEntity<DataEntityList>> getDataEntities(@Valid final OffsetDateTime changedSince,
                                                                final ServerWebExchange exchange) {
        return cacheRepository.getServices(changedSince)
            .collectList()
            .map(mapper::map).map(ResponseEntity::ok);
    }
}
