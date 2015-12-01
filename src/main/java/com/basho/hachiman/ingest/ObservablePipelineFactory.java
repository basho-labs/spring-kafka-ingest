package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.fn.RiakMapToPipeline;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.FetchMap;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.collections.api.map.ConcurrentMutableMap;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.Scheduler;
import rx.Subscription;
import rx.schedulers.Schedulers;
import rx.subjects.BehaviorSubject;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by jbrisbin on 12/1/15.
 */
@Component
public class ObservablePipelineFactory implements FactoryBean<Observable<Pipeline>> {

  private static final Logger LOG = LoggerFactory.getLogger(ObservablePipelineFactory.class);

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

  private RiakCluster               cluster;
  private RiakClient                client;
  private BehaviorSubject<Pipeline> pipelineSubject;
  private Subscription              pollerSubscription;

  private final ObjectMapper      mapper;
  private final RiakMapToPipeline rmtp;

  private final ConcurrentMutableMap<String, Pipeline> pipelines = ConcurrentHashMap.newMap();

  @Autowired
  public ObservablePipelineFactory(ObjectMapper mapper,
                                   RiakMapToPipeline rmtp) {
    this.mapper = mapper;
    this.rmtp = rmtp;
  }

  @PostConstruct
  public void init() throws Exception {
    List<RiakNode> nodes = FastList.newListWith(riakHosts.split(","))
                                   .collect(s -> s.split(":"))
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

    this.pipelineSubject = BehaviorSubject.create();

    Scheduler.Worker worker = Schedulers.newThread().createWorker();

    ListKeys listKeys = new ListKeys.Builder(new Namespace(ingestGroup, riakBucketName))
        .withTimeout(riakTimeout)
        .build();

    if (LOG.isInfoEnabled()) {
      LOG.info("Scheduling a polling job every {}s", riakPollInterval);
    }
    pollerSubscription = worker.schedulePeriodically(
        () -> client.executeAsync(listKeys)
                    .addListener(listFut -> {
                      for (Location loc : listFut.getNow()) {
                        FetchMap fetch = new FetchMap.Builder(loc)
                            .withTimeout(riakTimeout)
                            .build();

                        client.executeAsync(fetch)
                              .addListener(fetchFut -> {
                                RiakMap  rawMap      = fetchFut.getNow().getDatatype();
                                Pipeline newPipeline = rmtp.call(rawMap);
                                Pipeline oldPipeline = pipelines.putIfAbsent(newPipeline.getName(),
                                                                             newPipeline);
                                if (null == oldPipeline || !oldPipeline.equals(newPipeline)) {
                                  pipelineSubject.onNext(newPipeline);

                                  if (LOG.isDebugEnabled()) {
                                    try {
                                      LOG.debug("Pipeline configuration has changed. Old: {}, New: {}",
                                                mapper.writeValueAsString(oldPipeline),
                                                mapper.writeValueAsString(newPipeline));
                                    } catch (IOException e) {
                                      LOG.error(e.getMessage(), e);
                                    }
                                  }
                                }

                              });
                      }
                    }),
        0,
        riakPollInterval,
        TimeUnit.SECONDS
    );
  }

  @Override
  public Observable<Pipeline> getObject() throws Exception {
    return pipelineSubject;
  }

  @Override
  public Class<?> getObjectType() {
    return Observable.<Pipeline>create(sub -> {
    }).getClass();
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

}
