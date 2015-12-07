package com.basho.hachiman.ingest.kafka;

import kafka.message.MessageAndMetadata;

public class Record {
  
  private String key;
  private String value;

  public Record(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }


  public static Record fromKafkaMessage(MessageAndMetadata<String, String> message) {
    return new Record(message.key(), message.message());
  }
  
}
