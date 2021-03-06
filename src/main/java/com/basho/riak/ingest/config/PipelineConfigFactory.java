package com.basho.riak.ingest.config;

import com.gs.collections.impl.set.mutable.UnifiedSet;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static org.springframework.util.StringUtils.commaDelimitedListToStringArray;

/**
 * Component that creates a {@link PipelineConfig} from JSON stored in a property based on the ingest group.
 */
@Component
public class PipelineConfigFactory implements FactoryBean<PipelineConfig> {

  @Value("${riak.ingest.group}")
  private String ingestGroup;
  @Value("${riak.ingest.kafka.topic}")
  private String kafkaTopic;
  @Value("${riak.ingest.kafka.zookeepers}")
  private String kafkaZookeepers;
  @Value("${riak.ingest.riak.bucket}")
  private String riakBucket;
  @Value("${riak.ingest.riak.hosts}")
  private String riakHosts;
  @Value("${riak.ingest.riak.schema}")
  private String riakSchema;

  @Override
  public PipelineConfig getObject() throws Exception {
    return new PipelineConfig()
        .setName(ingestGroup)
        .setKafka(new KafkaConfig()
                      .setTopic(kafkaTopic)
                      .setZookeepers(UnifiedSet.newSetWith(commaDelimitedListToStringArray(kafkaZookeepers))))
        .setRiak(new RiakConfig()
                     .setBucket(riakBucket)
                     .setSchema(riakSchema)
                     .setHosts(UnifiedSet.newSetWith(commaDelimitedListToStringArray(riakHosts))));
  }

  @Override
  public Class<?> getObjectType() {
    return PipelineConfig.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

}
