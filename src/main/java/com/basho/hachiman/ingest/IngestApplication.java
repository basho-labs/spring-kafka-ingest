package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.archaius.InMemoryConfigurationSource;
import com.basho.hachiman.ingest.config.KafkaConfig;
import com.basho.hachiman.ingest.config.RiakConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.config.DynamicStringProperty;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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

  @Value("${hachiman.ingest.group}.")
  private String ingestGroup;

  @Value("${hachiman.ingest.checkСonfigRateMs}")
  private int checkConfigRateMs;

  @Autowired
  private InMemoryConfigurationSource source;

  @Autowired
  private DynamicPropertyFactory dynamicProps;

  @Autowired
  private ObjectMapper jsonMapper;

  @Override
  public void run(String... args) throws Exception {
    final DynamicStringProperty prop = dynamicProps.getStringProperty("pipeline", "");

    Observable<Object> configStream = Observable.create(observer -> {
      prop.addCallback(() -> {
        try {
          Pipeline newPipelineConf = jsonMapper.readValue(prop.get(), Pipeline.class);
          observer.onNext(newPipelineConf);
        } catch (Exception e) {
          observer.onError(e);
        }
      });
    });
    configStream.forEach(x -> System.out.println(x));

    String initialPipelineConf = jsonMapper.writeValueAsString(createPipeline("test1"));
    source.putSetting("pipeline", initialPipelineConf);

    Thread.sleep(1000);

    String changedPipelineConf = jsonMapper.writeValueAsString(createPipeline("test2"));
    source.putSetting("pipeline", changedPipelineConf);

    Thread.sleep(1000);
  }

  private Pipeline createPipeline(String name) {
    Pipeline pipeline = new Pipeline(
        name,
        new KafkaConfig("topic", UnifiedSet.newSetWith("khost1", "khost2")),
        new RiakConfig("bucket", UnifiedSet.newSetWith("rhost1", "rhost2"))
    );

    return pipeline;
  }

  public static void main(String... args) {
    ConfigurableApplicationContext ctx = SpringApplication.run(IngestApplication.class, args);
    ctx.close();
  }

}
