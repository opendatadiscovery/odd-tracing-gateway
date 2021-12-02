package org.opendatadiscovery.testapp.config;

import lombok.Data;

@Data
public class DynamoProperties {
  private String tableName = "clients";
  private String endpoint;
  private String accessKeyId;
  private String secretAccessKey;
  private String region = "eu-central-1";
  private boolean tokenFile = false;
}
