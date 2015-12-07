package com.basho.hachiman.ingest.config;

import java.util.Set;

/**
 * Created by jbrisbin on 11/20/15.
 */
public class KafkaConfig {

  private String      topic;
  private Set<String> brokers;

  public KafkaConfig() {
  }

  public KafkaConfig(String topic, Set<String> brokers) {
    this.topic = topic;
    this.brokers = brokers;
  }

  public String getTopic() {
    return topic;
  }

  public KafkaConfig setTopic(String topic) {
    this.topic = topic;
    return this;
  }

  //TODO: rename to Zookeepers
  public Set<String> getBrokers() {
    return brokers;
  }

  public KafkaConfig setBrokers(Set<String> brokers) {
    this.brokers = brokers;
    return this;
  }

  @Override
  public String toString() {
    return "KafkaConfig [topic=" + topic + ", brokers=" + brokers + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    KafkaConfig that = (KafkaConfig) o;

    if (topic != null ? !topic.equals(that.topic) : that.topic != null) return false;
    return !(brokers != null ? !brokers.equals(that.brokers) : that.brokers != null);
  }

  @Override
  public int hashCode() {
    int result = topic != null ? topic.hashCode() : 0;
    result = 31 * result + (brokers != null ? brokers.hashCode() : 0);
    return result;
  }

}
