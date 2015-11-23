package com.basho.hachiman.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import rx.Observable;
import rx.Subscription;
import rx.functions.Action1;
import rx.observables.AbstractOnSubscribe;

/**
 * Created by jbrisbin on 11/20/15.
 */
@SpringBootApplication
public class IngestApplication implements CommandLineRunner {

  @Value("hachiman.ingest.${hachiman.ingest.group}.")
  private String ingestGroup;

  @Autowired
  public ObjectMapper jsonMapper;

  @Bean
  public DynamicPropertyFactory dynamicProps() {
    return DynamicPropertyFactory.getInstance();
  }

  @Override
  public void run(String... args) throws Exception {

  }

  public static void main(String... args) {
    SpringApplication.run(IngestApplication.class, args);
  }

}
