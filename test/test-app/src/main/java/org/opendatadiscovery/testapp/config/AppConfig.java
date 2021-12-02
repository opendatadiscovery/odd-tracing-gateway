package org.opendatadiscovery.testapp.config;

import java.net.URI;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

@Configuration
public class AppConfig {

  @Bean
  @ConfigurationProperties(prefix = "dynamodb")
  public DynamoProperties dynamoProperties() {
    return new DynamoProperties();
  }

  @Bean
  public DynamoDbClient ddb(DynamoProperties dynamoProperties) {
    DynamoDbClientBuilder builder = DynamoDbClient.builder();
    if (dynamoProperties.getEndpoint() != null) {
      builder.endpointOverride(URI.create(dynamoProperties.getEndpoint()));
    }
    if (dynamoProperties.getAccessKeyId() != null
        && dynamoProperties.getSecretAccessKey() != null) {
      builder.credentialsProvider(
          StaticCredentialsProvider.create(
              AwsBasicCredentials
                  .create(dynamoProperties.getAccessKeyId(), dynamoProperties.getSecretAccessKey())
          )
      );
    }

    if (dynamoProperties.getRegion() != null) {
      builder.region(
          Region.of(dynamoProperties.getRegion())
      );
    }

    if (dynamoProperties.isTokenFile()) {
      builder.credentialsProvider(
          WebIdentityTokenFileCredentialsProvider.create()
      );
    }
    final DynamoDbClient build = builder.build();
    createTableIfNeed(build, dynamoProperties);
    return build;
  }

  public void createTableIfNeed(DynamoDbClient dbClient, DynamoProperties properties) {
    final ListTablesResponse listTablesResponse = dbClient.listTables();
    if (!listTablesResponse.hasTableNames() || !listTablesResponse.tableNames().contains(properties.getTableName())) {
      final CreateTableResponse response = dbClient.createTable(
          CreateTableRequest.builder()
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("id")
                      .attributeType(ScalarAttributeType.N)
                      .build()
              )
              .keySchema(
                  KeySchemaElement.builder()
                      .attributeName("id")
                      .keyType(KeyType.HASH)
                      .build()
              )
              .provisionedThroughput(ProvisionedThroughput.builder()
                  .readCapacityUnits(10L)
                  .writeCapacityUnits(10L)
                  .build())
              .tableName(properties.getTableName())
              .build()
      );
    }
  }

}
