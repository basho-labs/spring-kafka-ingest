package com.basho.hachiman.ingest.kafka;

import java.util.Properties;

import rx.Observable;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;


public class RxKafkaConnector {
  
  private ConsumerConnector consumer = null;
  
  public RxKafkaConnector(ConsumerConfig config) {
    consumer = Consumer.createJavaConsumerConnector(config);
  }
  
  public <K, V> Observable<MessageAndMetadata<K, V>> createObservable( 
      String topic, 
      Decoder<K> keyDecoder,
      Decoder<V> valueDecoder) {
    KafkaStream<K, V> kafkaStream = consumer.createMessageStreamsByFilter(
        new Whitelist(topic), 
        1,
        keyDecoder,
        valueDecoder).iterator().next();
    
    
    return Observable.from(kafkaStream);
  }
  
  public Observable<String> createStringMessageObservable(String topic) {
    return createObservable(topic, new StringDecoder(null), new StringDecoder(null))
        .map(m -> m.message());
  }
  
  public static ConsumerConfig createConfig(String group, String zookeepers, boolean autocommit, boolean startFromLatest) {
    Properties props = new Properties();
    props.put("group.id", group);
    props.put("zookeeper.connect", zookeepers);
    props.put("auto.offset.reset", startFromLatest ? "largest" : "smallest");
    props.put("auto.commit.enable", "" + autocommit);
    return new ConsumerConfig(props);
  }
  
  
  public void shutdown() {
    if (consumer != null)
      consumer.shutdown();
  }

}
