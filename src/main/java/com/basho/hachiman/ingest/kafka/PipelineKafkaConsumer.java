package com.basho.hachiman.ingest.kafka;

import com.basho.hachiman.ingest.config.PipelineConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.functions.Action1;

/**
 * Created by jbrisbin on 12/3/15.
 */
@Component
public class PipelineKafkaConsumer implements Action1<PipelineConfig> {

  @Value("${hachiman.ingest.group}")
  private String ingestGroup;
  @Value("${hachiman.ingest.kafka.zookeepers}")
  private String zookeepers;

  @Override
  public void call(PipelineConfig pipelineConfig) {

  }
}
