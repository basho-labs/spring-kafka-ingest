package com.basho.hachiman.ingest.kafka;


import com.basho.hachiman.ingest.config.KafkaConfig;
import com.basho.hachiman.ingest.config.PipelineConfig;
import kafka.consumer.ConsumerConfig;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.functions.Func2;
import rx.subjects.BehaviorSubject;

@Component
public class PipelineToObservable implements Func2<PipelineConfig, BehaviorSubject<Observable<Record>>, RxKafkaConnector> {

  @Override
  public RxKafkaConnector call(PipelineConfig p, BehaviorSubject<Observable<Record>> subject) {
    KafkaConfig kafkaConfig = p.getKafka();
    ConsumerConfig consumerConfig = RxKafkaConnector.createConfig(
        p.getName(), 
        kafkaConfig.getBrokers(), 
        true, 
        true);
    RxKafkaConnector connector = new RxKafkaConnector(consumerConfig);
    Observable<Record> observable = connector.createObservable(kafkaConfig.getTopic());
    subject.onNext(observable);
    return connector;    
  }

}
