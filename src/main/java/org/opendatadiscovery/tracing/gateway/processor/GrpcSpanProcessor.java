package org.opendatadiscovery.tracing.gateway.processor;

import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.trace.v1.Span;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.opendatadiscovery.adapter.contract.model.DataEntityType;
import org.opendatadiscovery.oddrn.Generator;
import org.opendatadiscovery.oddrn.model.GrpcServicePath;
import org.opendatadiscovery.oddrn.model.OddrnPath;
import org.opendatadiscovery.tracing.gateway.model.NameOddrn;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.opendatadiscovery.tracing.gateway.util.AnyValueUtil;
import org.springframework.stereotype.Service;

import static org.opendatadiscovery.tracing.gateway.util.AnyValueUtil.toMap;

@Service
@AllArgsConstructor
public class GrpcSpanProcessor implements SpanProcessor {
    private final Generator generator;

    @Override
    public boolean accept(final String library) {
        return library.startsWith("io.opentelemetry.grpc");
    }

    @Override
    public List<ServiceOddrns> process(final List<Span> spans,
                                       final Map<String, AnyValue> keyValue,
                                       final NameOddrn nameOddrn) {
        final Set<String> inputs = new HashSet<>();
        final Set<String> outputs = new HashSet<>();
        final List<ServiceOddrns> oddrns = new ArrayList<>();

        for (final Span span : spans) {
            final Map<String, AnyValue> attributes = toMap(span.getAttributesList());
            final Optional<String> service =
                Optional.ofNullable(attributes.get("rpc.service")).map(AnyValue::getStringValue);
            final Optional<String> method =
                Optional.ofNullable(attributes.get("rpc.method")).map(AnyValue::getStringValue);
            if (service.isPresent() && method.isPresent()) {
                final GrpcServicePath grpcPath = GrpcServicePath.builder()
                    .host("empty")
                    .service(service.get())
                    .method(method.get())
                    .build();
                final GrpcServicePath groupPath = grpcPath.toBuilder().method("").build();

                final String pathOddrn = generate(grpcPath);
                final String groupOddrn = generateService(groupPath);

                if (span.getKind().equals(Span.SpanKind.SPAN_KIND_SERVER)) {
                    outputs.add(pathOddrn);
                    oddrns.add(
                        ServiceOddrns.builder()
                            .name(String.format("%s/%s", service.get(), method.get()))
                            .oddrn(pathOddrn)
                            .groupOddrn(groupOddrn)
                            .groupName(String.format("%s", service.get()))
                            .groupType(DataEntityType.API_SERVICE)
                            .serviceType(DataEntityType.API_CALL)
                            .inputs(Set.of())
                            .outputs(Set.of(nameOddrn.getOddrn()))
                            .metadata(
                                Map.of(
                                    "rpc.service", service.get(),
                                    "rpc.method", method.get()
                                )
                            )
                            .build()
                    );
                } else if (span.getKind().equals(Span.SpanKind.SPAN_KIND_CLIENT)) {
                    inputs.add(pathOddrn);
                }
            }
        }

        oddrns.add(
            ServiceOddrns.builder()
                .inputs(inputs)
                .outputs(outputs)
                .oddrn(nameOddrn.getOddrn())
                .name(nameOddrn.getName())
                .version(nameOddrn.getVersion())
                .metadata(AnyValueUtil.toStringMap(keyValue))
                .build()
        );
        return oddrns;
    }

    @SneakyThrows
    private String generate(final OddrnPath path) {
        return generator.generate(path, "method");
    }

    @SneakyThrows
    private String generateService(final OddrnPath path) {
        return generator.generate(path, "service");
    }
}
