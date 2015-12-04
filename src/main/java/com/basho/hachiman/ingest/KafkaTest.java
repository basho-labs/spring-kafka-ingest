package com.basho.hachiman.ingest;

import java.io.IOException;

import rx.Observable;

import com.basho.hachiman.ingest.kafka.Record;
import com.basho.hachiman.ingest.kafka.RxKafkaConnector;
import com.gs.collections.impl.set.mutable.UnifiedSet;

public class KafkaTest {

  public static void main(String[] args) throws IOException {
    String group = "test";
    String zookepers = args[0];
    String topic = args[1];
    
    RxKafkaConnector connector = new RxKafkaConnector(RxKafkaConnector.createConfig(
        group, UnifiedSet.newSetWith(zookepers.split(",")), false, false));
    
    Observable<Record> messageStream = connector.createObservable(topic);
    
    messageStream.forEach(System.out::println);
    
    connector.close();
  }

}
