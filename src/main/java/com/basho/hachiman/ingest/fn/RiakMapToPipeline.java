package com.basho.hachiman.ingest.fn;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.config.KafkaConfig;
import com.basho.hachiman.ingest.config.RiakConfig;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.basho.riak.client.core.util.BinaryValue;
import com.gs.collections.impl.set.mutable.UnifiedSet;
import org.springframework.stereotype.Component;
import rx.functions.Func1;

/**
 * Created by jbrisbin on 11/30/15.
 */
@Component
public class RiakMapToPipeline implements Func1<RiakMap, PipelineConfig> {

  private static final Func1<RiakMap, KafkaConfig> TO_KAFKA_CONFIG = riakMap -> new KafkaConfig()
      .setTopic(riakMap.getRegister("topic").toString())
      .setBrokers(UnifiedSet.newSet(riakMap.getSet("brokers").view())
                            .collect(BinaryValue::toString));
  private static final Func1<RiakMap, RiakConfig>  TO_RIAK_CONFIG  = riakMap -> new RiakConfig()
      .setBucket(riakMap.getRegister("bucket").toString())
      .setHosts(UnifiedSet.newSet(riakMap.getSet("hosts").view())
                          .collect(BinaryValue::toString));

  public RiakMapToPipeline() {
  }

  @Override
  public PipelineConfig call(RiakMap riakMap) {
    PipelineConfig pipelineConfig = new PipelineConfig();

    pipelineConfig.setName(riakMap.getRegister("name").toString());

    RiakMap rmkc = riakMap.getMap("kafka");
    pipelineConfig.setKafka(TO_KAFKA_CONFIG.call(rmkc));

    RiakMap rmrc = riakMap.getMap("riak");
    pipelineConfig.setRiak(TO_RIAK_CONFIG.call(rmrc));

    return pipelineConfig;
  }

}
