package com.basho.hachiman.ingest.kafka;


import com.basho.hachiman.ingest.config.PipelineConfig;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.Observable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Properties;
import java.util.function.Supplier;

import static org.springframework.util.StringUtils.collectionToCommaDelimitedString;

/**
 * Component that creates an {@link Observable} for each topic.
 */
@Component
public class RxKafkaConnector implements Supplier<Observable<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(RxKafkaConnector.class);

  private final StringDecoder decoder = new StringDecoder(null);

  private final PipelineConfig pipelineConfig;

  private ConsumerConnector           consumer;
  private KafkaStream<String, String> kafkaStream;

  @Autowired
  public RxKafkaConnector(PipelineConfig pipelineConfig) {
    this.pipelineConfig = pipelineConfig;
  }

  @PostConstruct
  public void init() {
    this.consumer = Consumer.createJavaConsumerConnector(createConfig(pipelineConfig));
    this.kafkaStream = consumer.createMessageStreamsByFilter(
        new Whitelist(pipelineConfig.getKafka().getTopic()),
        1,
        decoder,
        decoder
    ).iterator().next();
  }

  @PreDestroy
  public void cleanup() {
    consumer.shutdown();
  }

  @Override
  public Observable<String> get() {
    return Observable.from(kafkaStream)
                     .doOnCompleted(() -> {
                       if (LOG.isDebugEnabled()) {
                         LOG.debug("Stream complete. Shutting down ConsumerConnector...");
                       }
                       consumer.shutdown();
                     })
                     .map(MessageAndMetadata::message);
  }

  public static ConsumerConfig createConfig(PipelineConfig pipeline) {
    String group      = pipeline.getName();
    String zookeepers = collectionToCommaDelimitedString(pipeline.getKafka().getZookeepers());

    Properties props = new Properties();
    props.put("group.id", group);
    props.put("zookeeper.connect", zookeepers);
    props.put("auto.offset.reset", "largest");
    props.put("auto.commit.enable", "true");

    return new ConsumerConfig(props);
  }

}
