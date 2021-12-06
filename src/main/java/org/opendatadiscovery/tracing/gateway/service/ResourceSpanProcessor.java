package org.opendatadiscovery.tracing.gateway.service;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationLibrary;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.InstrumentationLibrarySpans;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.opendatadiscovery.tracing.gateway.processor.SpanProcessor;
import org.opendatadiscovery.tracing.gateway.repository.CacheRepository;
import org.opendatadiscovery.tracing.gateway.resolver.ServiceNameResolver;
import org.opendatadiscovery.tracing.gateway.util.AnyValueUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import static org.opendatadiscovery.tracing.gateway.util.AnyValueUtil.toMap;

@Service
@Slf4j
public class ResourceSpanProcessor {
    private final Map<String, SpanProcessor> processors;
    private final List<ServiceNameResolver> nameResolvers;
    private final CacheRepository repository;

    @Autowired
    public ResourceSpanProcessor(
        final List<SpanProcessor> processorsList,
        final List<ServiceNameResolver> nameResolvers,
        final CacheRepository repository) {
        this.repository = repository;
        this.nameResolvers = nameResolvers.stream()
            .sorted(Comparator.comparingInt(ServiceNameResolver::priority))
            .collect(Collectors.toList());
        this.processors = processorsList.stream()
            .flatMap(p -> p.libraries().stream().map(l -> Tuples.of(l, p)))
            .collect(Collectors.toMap(
                Tuple2::getT1,
                Tuple2::getT2
            ));
    }

    public Mono<Boolean> process(final List<ResourceSpans> spans) {
        log.info("Processing {} spans", spans.size());
        return Flux.fromIterable(
            spans
        ).flatMap(this::process).collectList().map(s -> true);
    }

    public Mono<Boolean> process(final ResourceSpans spans) {
        final Resource resource = spans.getResource();
        final Map<String, AnyValue> keyValueMap = toMap(resource.getAttributesList());

        NameOddrn serviceName = NameOddrn.builder().oddrn("unknown").name("unknown").build();
        log.info("Finding name in {} resolvers", nameResolvers.size());

        for (final ServiceNameResolver nameResolver : nameResolvers) {
            final Optional<NameOddrn> resolved = nameResolver.resolve(keyValueMap);
            if (resolved.isPresent()) {
                serviceName = resolved.get();
                break;
            }
        }

        final NameOddrn serviceFullName = serviceName;
        log.info("Service name is {}", serviceFullName);
        log.info("spans size: {}", spans.getInstrumentationLibrarySpansList().size());

        return Flux.fromIterable(
            spans.getInstrumentationLibrarySpansList()
        ).flatMap(s -> process(s, keyValueMap, serviceFullName)).collectList().map(s -> true);
    }

    public Mono<Boolean> process(final InstrumentationLibrarySpans spans,
                                 final Map<String, AnyValue> keyValue, final NameOddrn service) {
        final InstrumentationLibrary library = spans.getInstrumentationLibrary();
        log.info("library name: {}", library.getName());
        try {
            final SpanProcessor spanProcessor = processors.get(library.getName());
            if (spanProcessor != null) {
                final ServiceOddrns oddrns = spanProcessor.process(spans.getSpansList(), keyValue)
                    .toBuilder()
                    .oddrn(service.getOddrn())
                    .name(service.getName())
                    .metadata(AnyValueUtil.toStringMap(keyValue))
                    .build();

                log.info("oddrns: {}", oddrns);
                return repository.add(oddrns).map(v -> true);
            } else {
                return Mono.just(true);
            }
        } catch (Throwable e) {
            log.error("Error processing: ", e);
            return Mono.just(true);
        }
    }
}
