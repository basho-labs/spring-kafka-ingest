package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.fn.RiakMapToPipeline;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.FetchMap;
import com.basho.riak.client.api.commands.kv.ListKeys;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.basho.riak.client.core.util.BinaryValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.gs.collections.api.map.ConcurrentMutableMap;
import com.gs.collections.impl.list.mutable.FastList;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextStoppedEvent;
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
public class ObservablePipelineConfigFactory implements FactoryBean<Observable<PipelineConfig>>,
    ApplicationListener<ContextStoppedEvent> {

  private static final Logger LOG = LoggerFactory.getLogger(ObservablePipelineConfigFactory.class);

  @Value("${hachiman.ingest.group}")
  private String ingestGroup;
  @Value("${hachiman.ingest.config.riak.timeout}")
  private int    riakTimeout;
  @Value("${hachiman.ingest.config.riak.pollInterval}")
  private int    riakPollInterval;
  @Value("${hachiman.ingest.riak.hosts}")
  private String riakHosts;

  private RiakCluster                     cluster;
  private RiakClient                      client;
  private BehaviorSubject<PipelineConfig> pipelineConfigSubject;
  private Scheduler.Worker                worker;
  private Subscription                    pollerSubscription;

  private final ObjectMapper      mapper;
  private final RiakMapToPipeline rmtp;

  private final ConcurrentMutableMap<String, PipelineConfig> pipelineConfigs = ConcurrentHashMap.newMap();
  private final ConcurrentMutableMap<String, BinaryValue>    contexts        = ConcurrentHashMap.newMap();

  @Autowired
  public ObservablePipelineConfigFactory(ObjectMapper mapper,
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

    this.pipelineConfigSubject = BehaviorSubject.create();

    this.worker = Schedulers.newThread().createWorker();

    ListKeys listKeys = new ListKeys.Builder(new Namespace(ingestGroup, ingestGroup))
        .withTimeout(riakTimeout)
        .build();

    if (LOG.isInfoEnabled()) {
      LOG.info("Scheduling a polling job every {}s", riakPollInterval);
    }
    pollerSubscription = worker.schedulePeriodically(
        () -> {
          try {
            for (Location loc : client.execute(listKeys)) {
              FetchMap fetch = new FetchMap.Builder(loc)
                  .withTimeout(riakTimeout)
                  .build();

              RiakMap        rawMap    = client.execute(fetch).getDatatype();
              PipelineConfig newConfig = rmtp.call(rawMap);
              PipelineConfig oldConfig = pipelineConfigs.putIfAbsent(newConfig.getName(), newConfig);
              if (null == oldConfig || !oldConfig.equals(newConfig)) {
                pipelineConfigSubject.onNext(newConfig);

                if (LOG.isDebugEnabled()) {
                  try {
                    LOG.debug("PipelineConfig configuration has changed. Old: {}, New: {}",
                              mapper.writeValueAsString(oldConfig),
                              mapper.writeValueAsString(newConfig));
                  } catch (IOException e) {
                    LOG.debug(e.getMessage(), e);
                  }
                }
              }
            }
          } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            pipelineConfigSubject.onError(e);
          }
        },
        0,
        riakPollInterval,
        TimeUnit.SECONDS
    );
    pipelineConfigSubject.doOnError(e -> {
      LOG.info("Unsubscribing poller job {}", pollerSubscription);
      pollerSubscription.unsubscribe();
    });
  }

  @Override
  public Observable<PipelineConfig> getObject() throws Exception {
    return pipelineConfigSubject;
  }

  @Override
  public Class<?> getObjectType() {
    return Observable.<PipelineConfig>create(sub -> {
    }).getClass();
  }

  @Override
  public boolean isSingleton() {
    return true;
  }

  @Override
  public void onApplicationEvent(ContextStoppedEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Shutting down RiakCluster {}", cluster);
    }
    cluster.shutdown();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Shutting down Worker poller {}", worker);
    }
    pollerSubscription.unsubscribe();
    worker.unsubscribe();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Shutting down Subject<PipelineConfig> {}", pipelineConfigSubject);
    }
    pipelineConfigSubject.onCompleted();
  }

}
