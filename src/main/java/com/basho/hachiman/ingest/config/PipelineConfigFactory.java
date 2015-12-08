package com.basho.hachiman.ingest.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Created by jbrisbin on 12/8/15.
 */
@Component
public class PipelineConfigFactory implements FactoryBean<PipelineConfig> {

  @Value("${hachiman.ingest.${hachiman.ingest.group:default}.config}")
  private String ingestConfig;

  private final ObjectMapper mapper;

  @Autowired
  public PipelineConfigFactory(ObjectMapper mapper) {
    this.mapper = mapper;
  }

  @Override
  public PipelineConfig getObject() throws Exception {
    return mapper.readValue(ingestConfig, PipelineConfig.class);
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
