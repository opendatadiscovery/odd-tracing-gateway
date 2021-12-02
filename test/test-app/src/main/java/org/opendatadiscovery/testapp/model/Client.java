package org.opendatadiscovery.testapp.model;

import java.time.LocalDateTime;
import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("client")
@Data
public class Client {
  @Id
  private long id;
  private String name;
  private LocalDateTime createdAt;
  private LocalDateTime updatedAt;
}
