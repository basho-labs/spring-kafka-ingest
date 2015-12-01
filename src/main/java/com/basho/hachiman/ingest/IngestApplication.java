package com.basho.hachiman.ingest;

import com.basho.riak.client.api.RiakClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import rx.Observable;

/**
 * Created by jbrisbin on 11/20/15.
 */
@SpringBootApplication
public class IngestApplication implements CommandLineRunner {

  private static final Logger LOG = LoggerFactory.getLogger(IngestApplication.class);

  @Autowired
  private Observable<Pipeline> pipelines;
  @Autowired
  private RiakClient           client;

  @Override
  public void run(String... args) throws Exception {
    pipelines.subscribe(pipeline -> LOG.info("Saw Pipeline: {}", pipeline));
  }

  public static void main(String... args) {
    ConfigurableApplicationContext ctx = SpringApplication.run(IngestApplication.class, args);
    //ctx.close();
  }

}
