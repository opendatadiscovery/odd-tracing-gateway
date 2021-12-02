package org.opendatadiscovery.testapp.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaReader {
  @KafkaListener(
      topics = "clients",
      groupId = "clients-reader"
  )
  public void listener(ConsumerRecord<Integer, String> record) {
    System.out.println(record);
  }
}
