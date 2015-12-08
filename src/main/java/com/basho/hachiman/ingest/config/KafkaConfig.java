package com.basho.hachiman.ingest.config;

import java.util.Set;

/**
 * Domain object that encapsulates the configuration necessary to connect to Kafka.
 */
public class KafkaConfig {

  private String      topic;
  private Set<String> zookeepers;

  public KafkaConfig() {
  }

  public KafkaConfig(String topic, Set<String> zookeepers) {
    this.topic = topic;
    this.zookeepers = zookeepers;
  }

  public String getTopic() {
    return topic;
  }

  public KafkaConfig setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  public Set<String> getZookeepers() {
    return zookeepers;
  }

  public KafkaConfig setZookeepers(Set<String> zookeepers) {
    this.zookeepers = zookeepers;
    return this;
  }

  @Override
  public String toString() {
    return "KafkaConfig [topic=" + topic + ", zookeepers=" + zookeepers + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    KafkaConfig that = (KafkaConfig) o;

    if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
    return !(zookeepers != null ? !zookeepers.equals(that.zookeepers) : that.zookeepers != null);
  }

  @Override
  public int hashCode() {
    int result = topic != null ? topic.hashCode() : 0;
    result = 31 * result + (zookeepers != null ? zookeepers.hashCode() : 0);
    return result;
  }

}
