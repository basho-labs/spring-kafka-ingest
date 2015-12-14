package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.config.PipelineConfigFactory;
import com.basho.hachiman.ingest.kafka.RxKafkaConnector;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Created by jbrisbin on 11/20/15.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = IngestApplication.class)
public class KafkaConsumerUnitTests {

  @Autowired
  RxKafkaConnector app;

  @Autowired
  PipelineConfigFactory pipelineConfigFactory;

  @Value("${hachiman.ingest.${hachiman.ingest.group:default}.kafka.brokers}")
  private String kafkaBrokers;

  private PipelineConfig pipelineConfig;
  private ConsumerConfig consumerConfig;
  private ConsumerConnector consumer;
  private KafkaStream<String, String> kafkaStream;

  private final StringDecoder decoder = new StringDecoder(null);

  @Before
  public void setup() throws Exception {
    KafkaProducer<String, String> producer = new KafkaProducer<String, String>(createProducerConfig());
    String[] content = new String(Files.readAllBytes(Paths.get(getClass().getResource("/data/2015.json")
            .toURI()))).split("\n");
    pipelineConfig = pipelineConfigFactory.getObject();
    for(String message : content) {
      producer.send(new ProducerRecord<String, String>(pipelineConfig.getKafka().getTopic(), message));
    }
    consumerConfig = RxKafkaConnector.createConfig(pipelineConfig);
    consumer = Consumer.createJavaConsumerConnector(consumerConfig);
  }

  @After
  public void cleanup() {
    consumer.shutdown();
  }

  @Test
  public void canConsumeFromTopicAndParition() {
    consumer.createMessageStreamsByFilter(
            new Whitelist(pipelineConfig.getKafka().getTopic()),
            1, decoder, decoder).iterator().next();
    System.out.println(pipelineConfig.getName());
    System.out.println(pipelineConfig.getKafka());
    System.out.println(pipelineConfig.getRiak());
  }

  private Properties createProducerConfig() {

    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);

    return props;
  }

}
