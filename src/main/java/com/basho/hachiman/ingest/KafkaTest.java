package com.basho.hachiman.ingest;

import rx.Observable;

import com.basho.hachiman.ingest.kafka.RxKafkaConnector;

public class KafkaTest {

  public static void main(String[] args) {
    String group = "test";
    String zookepers = args[0];
    String topic = args[1];
    
    RxKafkaConnector connector = new RxKafkaConnector(RxKafkaConnector.createConfig(
        group, zookepers, false, false));
    
    Observable<String> messageStream = connector.createStringMessageObservable(topic);
    
    messageStream.forEach(System.out::println);
  }

}
