package com.basho.hachiman.ingest.config;

/**
 * Domain object that encapsulates the configuration settings for an ingestion pipeline.
 */
public class PipelineConfig {

  private String      name;
  private KafkaConfig kafka;
  private RiakConfig  riak;

  public PipelineConfig() {
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
    return "PipelineConfig [name=" + name + ", kafka=" + kafka + ", riak=" + riak + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PipelineConfig pipelineConfig = (PipelineConfig) o;

    if (name != null ? !name.equals(pipelineConfig.name) : pipelineConfig.name != null) return false;
    if (kafka != null ? !kafka.equals(pipelineConfig.kafka) : pipelineConfig.kafka != null) return false;
    return !(riak != null ? !riak.equals(pipelineConfig.riak) : pipelineConfig.riak != null);
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (kafka != null ? kafka.hashCode() : 0);
    result = 31 * result + (riak != null ? riak.hashCode() : 0);
    return result;
  }

}
