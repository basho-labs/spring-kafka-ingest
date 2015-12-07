package com.basho.hachiman.ingest.fn;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.config.KafkaConfig;
import com.basho.hachiman.ingest.config.RiakConfig;
import com.basho.riak.client.api.commands.datatypes.MapUpdate;
import com.basho.riak.client.api.commands.datatypes.RegisterUpdate;
import com.basho.riak.client.api.commands.datatypes.SetUpdate;
import org.springframework.stereotype.Component;
import rx.functions.Func1;

/**
 * Created by jbrisbin on 12/1/15.
 */
@Component
public class PipelineToMapUpdate implements Func1<PipelineConfig, MapUpdate> {

  public static final Func1<KafkaConfig, MapUpdate> KAFKA_CONFIG_TO_MAPUPD = config -> {
    MapUpdate upd = new MapUpdate();

    upd.update("topic", new RegisterUpdate(config.getTopic()));
    SetUpdate brokersUpd = new SetUpdate();
    config.getBrokers().forEach(brokersUpd::add);
    upd.update("brokers", brokersUpd);

    return upd;
  };
  public static final Func1<RiakConfig, MapUpdate>  RIAK_CONFIG_TO_MAPUPD  = config -> {
    MapUpdate upd = new MapUpdate();

    upd.update("bucket", new RegisterUpdate(config.getBucket()));
    SetUpdate hostsUpd = new SetUpdate();
    config.getHosts().forEach(hostsUpd::add);
    upd.update("hosts", hostsUpd);

    return upd;
  };

  @Override
  public MapUpdate call(PipelineConfig pipelineConfig) {
    MapUpdate upd = new MapUpdate();

    upd.update("name", new RegisterUpdate(pipelineConfig.getName()));
    upd.update("kafka", KAFKA_CONFIG_TO_MAPUPD.call(pipelineConfig.getKafka()));
    upd.update("riak", RIAK_CONFIG_TO_MAPUPD.call(pipelineConfig.getRiak()));

    return upd;
  }
}
