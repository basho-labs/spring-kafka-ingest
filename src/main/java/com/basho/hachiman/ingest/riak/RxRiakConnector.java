package com.basho.hachiman.ingest.riak;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.kafka.RxKafkaConnector;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.timeseries.Row;
import com.gs.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Component for reacting to string-based messages and parsing them into a {@code List}, turning that into a {@link
 * Row}
 * and inserting that into Riak TS.
 */
@Component
public class RxRiakConnector implements Action1<Row> {

  private static final Logger LOG = LoggerFactory.getLogger(RxRiakConnector.class);

  private static final String ERROR_COUNT = "hachiman.ingest.errorCount";
  private static final String MSG_COUNT   = "hachiman.ingest.messageCount";

  private RiakCluster cluster;
  private RiakClient  client;

  private final PipelineConfig             pipelineConfig;
  private final StringToRowFunction        stringToRowFn;
  private final Observable<String>         messages;
  private final BehaviorSubject<Throwable> errorStream;
  private final CounterService             counters;

  private final AtomicBoolean running = new AtomicBoolean(false);

  @Autowired
  public RxRiakConnector(PipelineConfig pipelineConfig,
                         RxKafkaConnector kafkaConnector,
                         StringToRowFunction stringToRowFn,
                         BehaviorSubject<Throwable> errorStream,
                         CounterService counters) {
    this.pipelineConfig = pipelineConfig;
    this.stringToRowFn = stringToRowFn;
    this.errorStream = errorStream;
    this.messages = kafkaConnector.get();
    this.counters = counters;
  }

  @PostConstruct
  public void init() {
    if (LOG.isInfoEnabled()) {
      LOG.info("Connecting to Riak hosts: {}", pipelineConfig.getRiak().getHosts());
    }

    // Transform Set<String> of host:port to a List<RiakNode>
    List<RiakNode> nodes = FastList.newList(pipelineConfig.getRiak().getHosts())
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
    try {
      cluster = RiakCluster.builder(nodes).build();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e.getMessage(), e);
    }
    client = new RiakClient(cluster);
    if (running.compareAndSet(false, true)) {
      cluster.start();
    }

    messages.doOnCompleted(this::cleanup)
            .doOnError(ex -> {
              errorStream.onNext(ex);
              counters.increment(ERROR_COUNT);
            })
            .map(stringToRowFn)
            .subscribe(this);
  }

  @PreDestroy
  public void cleanup() {
    if (running.compareAndSet(true, false)) {
      cluster.shutdown();
    }
  }

  @Override
  public void call(Row row) {
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing row to Riak bucket {}", pipelineConfig.getRiak().getBucket());
      }
      client.execute(new Store.Builder(pipelineConfig.getRiak().getBucket())
                         .withRow(row)
                         .build());

      counters.increment(MSG_COUNT);
      counters.reset(ERROR_COUNT);
    } catch (Exception ex) {
      counters.increment(ERROR_COUNT);
      errorStream.onNext(ex);
      //LOG.error(ex.getMessage(), ex);
    }
  }

}
