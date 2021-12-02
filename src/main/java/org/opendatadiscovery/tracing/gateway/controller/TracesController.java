package org.opendatadiscovery.tracing.gateway.controller;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.ReactorTraceServiceGrpc;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.opendatadiscovery.tracing.gateway.service.ResourceSpanProcessor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@GrpcService
@Slf4j
@AllArgsConstructor
public class TracesController extends ReactorTraceServiceGrpc.TraceServiceImplBase {
    private final ResourceSpanProcessor processor;

    @Override
    public Mono<ExportTraceServiceResponse> export(final Mono<ExportTraceServiceRequest> request) {
        log.info("Received traces");
        return request.flatMap(r -> processor.process(r.getResourceSpansList()))
            .map(r -> ExportTraceServiceResponse.getDefaultInstance())
            .doOnError(e -> log.error("error", e));
    }
}
