package com.basho.hachiman.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import rx.subjects.BehaviorSubject;

/**
 * Simple Spring Boot application to bootstrap the ingester components.
 */
@SpringBootApplication
public class IngestApplication implements CommandLineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(IngestApplication.class);

  /**
   * Report all errors through a central stream.
   */
  @Bean
  public BehaviorSubject<Throwable> errorStream() {
    return BehaviorSubject.create();
  }

  @Override
  public void run(String... args) throws Exception {
    // By default just log errors we encounter.
    errorStream().subscribe(ex -> LOG.error(ex.getMessage(), ex));
  }

  public static void main(String... args) {
    SpringApplication.run(IngestApplication.class, args);
  }

}
