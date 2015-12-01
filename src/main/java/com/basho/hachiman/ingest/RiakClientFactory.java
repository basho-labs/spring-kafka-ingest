package com.basho.hachiman.ingest;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.gs.collections.impl.list.mutable.FastList;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by jbrisbin on 12/1/15.
 */
@Component
public class RiakClientFactory implements FactoryBean<RiakClient> {

  @Value("${hachiman.ingest.group}")
  private String ingestGroup;
  @Value("${hachiman.ingest.config.riak.timeout}")
  private int    riakTimeout;
  @Value("${hachiman.ingest.config.riak.pollInterval}")
  private int    riakPollInterval;
  @Value("${hachiman.ingest.config.riak.bucket}")
  private String riakBucketName;
  @Value("${hachiman.ingest.riak.hosts}")
  private String riakHosts;

  private RiakCluster cluster;
  private RiakClient  client;

  private final AtomicBoolean started = new AtomicBoolean(false);

  @PostConstruct
  public void init() throws Exception {
    List<RiakNode> nodes = FastList.newListWith(riakHosts.split(","))
                                   .collectIf(s -> !s.isEmpty(), s -> s.split(":"))
                                   .collect(s -> {
                                     try {
                                       return new RiakNode.Builder()
                                           .withRemoteAddress(s[0])
                                           .withRemotePort(Integer.valueOf(s[1]))
                                           .build();
                                     } catch (UnknownHostException e) {
                                       throw new IllegalArgumentException(e);
                                     }
                                   });
    this.cluster = RiakCluster.builder(nodes).build();
    this.client = new RiakClient(cluster);
    this.cluster.start();
  }

  @Override
  public RiakClient getObject() throws Exception {
    return client;
  }

  @Override
  public Class<?> getObjectType() {
    return RiakClient.class;
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

}
