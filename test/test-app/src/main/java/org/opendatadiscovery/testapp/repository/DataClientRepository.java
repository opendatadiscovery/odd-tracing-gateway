package org.opendatadiscovery.testapp.repository;

import org.opendatadiscovery.testapp.model.Client;
import org.springframework.data.repository.CrudRepository;

public interface DataClientRepository extends CrudRepository<Client, Long> {
}
