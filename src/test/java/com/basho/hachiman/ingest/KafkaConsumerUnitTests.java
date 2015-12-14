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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

/**
 * Created by jbrisbin on 11/20/15.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = IngestApplication.class)
public class KafkaConsumerUnitTests {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerUnitTests.class);

  @Autowired
  RxKafkaConnector app;

  @Autowired
  PipelineConfigFactory pipelineConfigFactory;

  @Value("${hachiman.ingest.kafka.brokers}")
  private String kafkaBrokers;

  private PipelineConfig                pipelineConfig;
  private ConsumerConfig                consumerConfig;
  private ConsumerConnector             consumer;
  private String                        topic;
  private KafkaStream<String, String>   kafkaStream;
  private KafkaProducer<String, String> producer;

  private final StringDecoder decoder = new StringDecoder(null);

  @Before
  public void setup() throws Exception {
    this.pipelineConfig = pipelineConfigFactory.getObject();
    this.producer = new KafkaProducer<>(createProducerConfig());
    this.topic = pipelineConfig.getKafka().getTopic();

    List<String> lines = Files.readAllLines(Paths.get(new ClassPathResource("/data/2015.json")
                                                          .getURI()));
    for (String message : lines) {
      producer.send(new ProducerRecord<>(topic, message));
    }
    this.consumerConfig = RxKafkaConnector.createConfig(pipelineConfig);
    this.consumer = Consumer.createJavaConsumerConnector(consumerConfig);
  }

  @After
  public void cleanup() {

  }

  @Test
  public void canConsumeFromTopicAndParition() {
    consumer.createMessageStreamsByFilter(
        new Whitelist(topic),
        1,
        decoder,
        decoder
    ).iterator().next();

    LOG.info(pipelineConfig.toString());
  }

  private Properties createProducerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
    return props;
  }

}
