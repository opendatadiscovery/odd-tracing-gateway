package org.opendatadiscovery.tracing.gateway.mapper;

import java.net.URI;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opendatadiscovery.adapter.contract.model.DataEntity;
import org.opendatadiscovery.adapter.contract.model.DataEntityList;
import org.opendatadiscovery.adapter.contract.model.DataEntityType;
import org.opendatadiscovery.adapter.contract.model.DataInput;
import org.opendatadiscovery.adapter.contract.model.DataTransformer;
import org.opendatadiscovery.adapter.contract.model.MetadataExtension;
import org.opendatadiscovery.tracing.gateway.config.AppProperties;
import org.opendatadiscovery.tracing.gateway.model.ServiceOddrns;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
@Slf4j
public class ServiceMapper {
    private final AppProperties properties;

    public DataEntityList map(final List<ServiceOddrns> oddrns) {
        log.info("oddrns: {}", oddrns);
        final DataEntityList dataEntityList = new DataEntityList();
        dataEntityList.setDataSourceOddrn(properties.getOddrn());
        dataEntityList.setItems(
            oddrns.stream().map(this::map).collect(Collectors.toList())
        );
        return dataEntityList;
    }

    public DataEntity map(final ServiceOddrns oddrns) {
        final DataEntity entity = new DataEntity();
        entity.setName(oddrns.getName());
        entity.setOddrn(oddrns.getOddrn());
        entity.setUpdatedAt(OffsetDateTime.ofInstant(oddrns.getUpdatedAt(), ZoneOffset.UTC));
        entity.setMetadata(
            List.of(
                new MetadataExtension()
                    .metadata(Collections.unmodifiableMap(oddrns.getMetadata()))
                    .schemaUrl(URI.create("http://unknown"))
            )
        );

        if (oddrns.getServiceType().equals(DataEntityType.MICROSERVICE)) {
            entity.setType(DataEntityType.MICROSERVICE);
            final DataTransformer dataTransformer = new DataTransformer();
            final boolean hasInputs = oddrns.getInputs() != null && oddrns.getInputs().size() > 0;
            final boolean hasOutputs = oddrns.getOutputs() != null && oddrns.getOutputs().size() > 0;

            dataTransformer.setInputs(hasInputs ? new ArrayList<>(oddrns.getInputs()) : List.of());
            dataTransformer.setOutputs(hasOutputs ? new ArrayList<>(oddrns.getOutputs()) : List.of());
            entity.setDataTransformer(dataTransformer);
        } else if (oddrns.getServiceType().equals(DataEntityType.API_CALL)) {
            entity.setType(DataEntityType.API_CALL);
            final DataInput dataInput = new DataInput();
            entity.setDataInput(dataInput);
        }

        return entity;
    }
}
