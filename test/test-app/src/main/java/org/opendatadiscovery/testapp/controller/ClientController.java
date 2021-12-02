package org.opendatadiscovery.testapp.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import org.opendatadiscovery.testapp.model.Client;
import org.opendatadiscovery.testapp.repository.DataClientRepository;
import org.opendatadiscovery.testapp.repository.DynamoClientRepository;
import org.opendatadiscovery.testapp.repository.JooqClientRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/clients")
@AllArgsConstructor
public class ClientController {
  private final JooqClientRepository repository;
  private final DynamoClientRepository dynamoRepository;
  private final KafkaTemplate<String, String> producer;
  private final ObjectMapper mapper;

  @GetMapping("/{id}")
  private Mono<Client> getClientById(@PathVariable Long id) {
    dynamoRepository.findById(id);
    return repository.findById(id).map(Mono::just).orElse(Mono.empty());
  }

  @GetMapping
  private Flux<Client> getAllClients() {
    return Flux.fromIterable(repository.findAll());
  }

  @PostMapping
  private Mono<Client> createClient(@RequestBody Client client) throws JsonProcessingException {
    dynamoRepository.save(client);
    final Client save = repository.save(client);
    return Mono.fromFuture(
        producer.send(
            "clients",
            Long.valueOf(save.getId()).toString(),
            mapper.writeValueAsString(client)
        ).completable()
    ).map(r -> save);
  }
}
