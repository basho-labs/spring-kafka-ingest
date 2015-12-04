package com.basho.hachiman.ingest.kafka;


import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;

import org.apache.commons.lang3.StringUtils;

import rx.Observable;


public class RxKafkaConnector {
  
  private ConsumerConnector consumer = null;
  
  public RxKafkaConnector(ConsumerConfig config) {
    consumer = Consumer.createJavaConsumerConnector(config);
  }
  
  public Observable<Record> createObservable(String topic) {
    
    StringDecoder decoder = new StringDecoder(null);
    KafkaStream<String, String> kafkaStream = consumer.createMessageStreamsByFilter(
        new Whitelist(topic), 
        1,
        decoder,
        decoder).iterator().next();
    
    return Observable
        .from(kafkaStream)
        .map(Record::fromKafkaMessage);
  }
  

  
  public static ConsumerConfig createConfig(String group, Iterable<String> zookeepers, boolean autocommit, boolean startFromLatest) {
    Properties props = new Properties();
    props.put("group.id", group);
    props.put("zookeeper.connect", StringUtils.join(zookeepers, ','));
    props.put("auto.offset.reset", startFromLatest ? "largest" : "smallest");
    props.put("auto.commit.enable", "" + autocommit);
    return new ConsumerConfig(props);
  }
  
  
  public void close() {
    if (consumer != null)
      consumer.shutdown();    
  }

}
