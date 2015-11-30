package com.basho.hachiman.ingest.config;

import java.util.List;

/**
 * Created by jbrisbin on 11/20/15.
 */
public class KafkaConfig {

    private String topic;
    private List<String> brokers;

    
    public KafkaConfig() { }
    
    public KafkaConfig(String topic, List<String> brokers) {
        this.topic = topic;
        this.brokers = brokers;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getBrokers() {
        return brokers;
    }

    public void setBrokers(List<String> brokers) {
        this.brokers = brokers;
    }

    @Override
    public String toString() {
        return "KafkaConfig [topic=" + topic + ", brokers=" + brokers + "]";
    }

    
    
    
}
