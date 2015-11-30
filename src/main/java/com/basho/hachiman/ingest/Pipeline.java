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

  public Pipeline() {
  }

  public Pipeline(String name, KafkaConfig kafka, RiakConfig riak) {
    this.name = name;
    this.kafka = kafka;
    this.riak = riak;
  }

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

  @Override
  public String toString() {
    return "Pipeline [name=" + name + ", kafka=" + kafka + ", riak=" + riak + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Pipeline pipeline = (Pipeline) o;

    if (name != null ? !name.equals(pipeline.name) : pipeline.name != null) return false;
    if (kafka != null ? !kafka.equals(pipeline.kafka) : pipeline.kafka != null) return false;
    return !(riak != null ? !riak.equals(pipeline.riak) : pipeline.riak != null);
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (kafka != null ? kafka.hashCode() : 0);
    result = 31 * result + (riak != null ? riak.hashCode() : 0);
    return result;
  }

}
