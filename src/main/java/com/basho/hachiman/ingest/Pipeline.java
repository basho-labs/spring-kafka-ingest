package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.config.KafkaConfig;
import com.basho.hachiman.ingest.config.RiakConfig;

/**
 * Created by jbrisbin on 11/20/15.
 */
public class Pipeline {

  private String      name;
  private KafkaConfig kafka;
  private RiakConfig  riak;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public KafkaConfig getKafka() {
    return kafka;
  }

  public void setKafka(KafkaConfig kafka) {
    this.kafka = kafka;
  }

  public RiakConfig getRiak() {
    return riak;
  }

  public void setRiak(RiakConfig riak) {
    this.riak = riak;
  }

}
