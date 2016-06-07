package com.basho.riak.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;

import com.basho.riak.client.core.NoNodesAvailableException;

import rx.subjects.BehaviorSubject;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

/**
 * Simple Spring Boot application to bootstrap the ingester components.
 */
@SpringBootApplication
public class IngestApplication {

  private static final Logger LOG = LoggerFactory.getLogger(IngestApplication.class);

  /**
   * Report all errors through a central stream.
   */
  @Bean
  public BehaviorSubject<Throwable> errorStream() {
    return BehaviorSubject.create();
  }

  public static void main(String... args) {
    ConfigurableApplicationContext ctx = SpringApplication.run(IngestApplication.class, args);
    IngestApplication              app = ctx.getBean(IngestApplication.class);

    // If encountering an IOException, kill the system and let cloud provider deal with restarts.
    app.errorStream().subscribe(ex -> {
      LOG.error(ex.getMessage(), ex);
      if (ex instanceof IOException 
          || (null != ex.getCause() && ex.getCause() instanceof NoNodesAvailableException)
          || (null != ex.getCause() && ex.getCause() instanceof IOException)) {
        ctx.close();
        while (ctx.isRunning()) {
          LockSupport.parkNanos(1);
        }
        System.exit(-1);
      }
    });
  }

}
