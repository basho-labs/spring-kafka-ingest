package com.basho.hachiman.ingest.kafka;

import org.springframework.stereotype.Component;

import kafka.consumer.ConsumerConfig;

import com.basho.hachiman.ingest.Pipeline;
import com.basho.hachiman.ingest.config.KafkaConfig;

import rx.Observable;
import rx.functions.Func1;

@Component
public class PipelineToObservable implements Func1<Pipeline, Observable<String>> {

  @Override
  public Observable<String> call(Pipeline p) {
    KafkaConfig kafkaConfig = p.getKafka();
    ConsumerConfig consumerConfig = RxKafkaConnector.createConfig(
        p.getName(), 
        kafkaConfig.getBrokers().iterator().next(), //TODO: support multiple items here
        true, 
        true);
    RxKafkaConnector connector = new RxKafkaConnector(consumerConfig);
    return connector.createStringMessageObservable(kafkaConfig.getTopic());
  }

}
