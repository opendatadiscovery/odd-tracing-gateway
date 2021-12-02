package org.opendatadiscovery.testapp.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.opendatadiscovery.testapp.config.DynamoProperties;
import org.opendatadiscovery.testapp.model.Client;
import org.springframework.stereotype.Repository;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;

@Repository
@RequiredArgsConstructor
public class DynamoClientRepositoryImpl implements DynamoClientRepository {
  private final DynamoDbClient dbClient;
  private final DynamoProperties properties;
  private final ObjectMapper mapper;

  @Override
  @SneakyThrows
  public Client save(Client entity) {
    final PutItemResponse response = dbClient.putItem(
        PutItemRequest.builder()
            .tableName(properties.getTableName())
            .item(
                Map.of(
                    "id",
                    AttributeValue.builder()
                        .n(Long.valueOf(entity.getId()).toString())
                        .build(),
                    "value",
                    AttributeValue.builder()
                        .s(mapper.writeValueAsString(entity))
                        .build()
                    )
            )
            .build()
    );
    return entity;
  }

  @Override
  @SneakyThrows
  public Optional<Client> findById(Long id) {
    final GetItemResponse response = dbClient.getItem(
        GetItemRequest.builder()
            .tableName(properties.getTableName())
            .key(
                Map.of(
                    "id",
                    AttributeValue.builder()
                        .n(id.toString())
                        .build()
                )
            )
            .build()
    );
    if (response.hasItem()) {
      return Optional.of(
          mapper.readValue(
            response.item().get("value").s(),
            Client.class
          )
      );
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Iterable<Client> findAll() {
    return null;
  }
}
