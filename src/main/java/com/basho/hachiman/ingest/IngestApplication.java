package com.basho.hachiman.ingest;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Created by jbrisbin on 11/20/15.
 */
@SpringBootApplication
public class IngestApplication implements CommandLineRunner {

  @Override
  public void run(String... args) throws Exception {

  }

  public static void main(String... args) {
    SpringApplication.run(IngestApplication.class, args);
  }

}
