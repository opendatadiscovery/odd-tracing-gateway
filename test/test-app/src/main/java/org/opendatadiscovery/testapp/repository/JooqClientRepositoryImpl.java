package org.opendatadiscovery.testapp.repository;

import static org.opendatadiscovery.testapp.dbmodel.tables.Client.CLIENT;

import java.util.Optional;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.opendatadiscovery.testapp.dbmodel.tables.records.ClientRecord;
import org.opendatadiscovery.testapp.model.Client;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class JooqClientRepositoryImpl implements JooqClientRepository {
  private final DSLContext context;

  @Override
  public Client save(Client entity) {
    return this.fromRecord(
        context.insertInto(CLIENT)
          .columns(CLIENT.NAME)
          .values(entity.getName())
          .returning().fetchOne()
    );
  }

  @Override
  public Optional<Client> findById(Long id) {
    return context.selectFrom(CLIENT).where(
        CLIENT.ID.eq(id)
    ).fetchOptional().map(this::fromRecord);
  }

  @Override
  public Iterable<Client> findAll() {
    return context.selectFrom(CLIENT)
        .fetchStream().map(this::fromRecord)
        .collect(Collectors.toList());
  }

  private Client fromRecord(ClientRecord record) {
    Client client = new Client();
    client.setId(record.getId());
    client.setName(record.getName());
    client.setUpdatedAt(record.getUpdatedAt());
    client.setCreatedAt(record.getCreatedAt());
    return client;
  }
}
