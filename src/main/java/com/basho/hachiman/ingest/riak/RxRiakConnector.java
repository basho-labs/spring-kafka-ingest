package com.basho.hachiman.ingest.riak;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.kafka.RxKafkaConnector;
import com.basho.hachiman.ingest.util.MetricUtils;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.kv.StoreValue;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import com.gs.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import rx.functions.Action1;
import rx.subjects.BehaviorSubject;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Component for reacting to string-based messages and parsing them into a {@code List}, turning that into a {@link
 * Row}
 * and inserting that into Riak TS.
 */
@Component
public class RxRiakConnector implements Action1<Row> {

  private static final Logger LOG = LoggerFactory.getLogger(RxRiakConnector.class);

  private RiakCluster cluster;
  private RiakClient  client;

  private final PipelineConfig             pipelineConfig;
  private final RxKafkaConnector           kafkaConnector;
  private final StringToRowFunction        stringToRowFn;
  private final BehaviorSubject<Throwable> errorStream;
  private final Location                   kvLocation;

  @Autowired
  private MetricUtils metricUtils;

  private final AtomicBoolean running = new AtomicBoolean(false);

  @Autowired
  public RxRiakConnector(PipelineConfig pipelineConfig,
                         RxKafkaConnector kafkaConnector,
                         StringToRowFunction stringToRowFn,
                         BehaviorSubject<Throwable> errorStream) {
    this.pipelineConfig = pipelineConfig;
    this.kafkaConnector = kafkaConnector;
    this.stringToRowFn = stringToRowFn;
    this.errorStream = errorStream;
    this.kvLocation = new Location(new Namespace(pipelineConfig.getRiak().getKvBucket(),
            pipelineConfig.getRiak().getKvBucket()),
            BinaryValue.create(pipelineConfig.getRiak().getKvKey().getBytes()));
  }

  @PostConstruct
  public void init() throws Exception {
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

    kafkaConnector.get()
                  .map(msg -> {
                    try {
                      return stringToRowFn.call(msg);
                    } catch (Throwable t) {
                      metricUtils.incErrCount();
                      errorStream.onNext(t);
                      return new Row();
                    }
                  })
                  .subscribe(this);

    metricUtils.incMsgCount(getRiakOffset());
  }

  public RiakClient getRiakClient() {
    return client;
  }

  @PreDestroy
  public void cleanup() {
    if (running.compareAndSet(true, false)) {
      cluster.shutdown();
    }
  }

  @Override
  public void call(Row row) {
    if (row.getCells().isEmpty()) {
      return;
    }
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing row to Riak bucket {}", pipelineConfig.getRiak().getBucket());
      }

      client.execute(new Store.Builder(pipelineConfig.getRiak().getBucket())
                         .withRow(row)
                         .build());
      final long offset = row.getCells().get(2).getTimestamp();
      saveRiakOffset(offset);

      metricUtils.resetErrCount();
    } catch (Exception ex) {
      LOG.error("Storing data error: ", ex);
      metricUtils.incErrCount();
      errorStream.onNext(ex);
    }
  }

  public long getRiakOffset() throws Exception {
    FetchValue fv = new FetchValue.Builder(kvLocation).build();
    FetchValue.Response response = getRiakClient().execute(fv);
    String timestamp = response.getValue(String.class);
    return timestamp != null ? Long.valueOf(timestamp) : 0;
  }

  public void saveRiakOffset(long offset) throws ExecutionException, InterruptedException {
    final RiakObject value = new RiakObject().setValue(BinaryValue.create(String.valueOf(offset)));
    final StoreValue storeCommand = new StoreValue.Builder(value).withLocation(kvLocation).build();
    client.execute(storeCommand);
  }
}
