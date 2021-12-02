package org.opendatadiscovery.testapp.repository;

import java.util.Optional;
import org.opendatadiscovery.testapp.model.Client;

public interface DynamoClientRepository {
  Client save(Client entity);

  Optional<Client> findById(Long id);

  Iterable<Client> findAll();
}
