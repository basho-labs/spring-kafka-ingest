package com.basho.hachiman.ingest.riak;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.kafka.RxKafkaConnector;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.timeseries.Store;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.Row;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import static org.springframework.util.StringUtils.commaDelimitedListToStringArray;

/**
 * Component for reacting to string-based messages and parsing them into a {@code List}, turning that into a {@link
 * Row}
 * and inserting that into Riak TS.
 */
@Component
public class RxRiakConnector implements Action1<String> {

  private static final Logger LOG = LoggerFactory.getLogger(RxRiakConnector.class);

  private static final String ERROR_COUNT = "hachiman.ingest.errorCount";
  private static final String MSG_COUNT   = "hachiman.ingest.messageCount";

  private String[]    schema;
  private RiakCluster cluster;
  private RiakClient  client;

  private final ObjectMapper               mapper;
  private final PipelineConfig             pipelineConfig;
  private final Observable<String>         messages;
  private final BehaviorSubject<Throwable> errorStream;
  private final CounterService             counters;

  @Autowired
  public RxRiakConnector(ObjectMapper mapper,
                         PipelineConfig pipelineConfig,
                         RxKafkaConnector kafkaConnector,
                         BehaviorSubject<Throwable> errorStream,
                         CounterService counters) {
    this.mapper = mapper;
    this.pipelineConfig = pipelineConfig;
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
    cluster.start();

    schema = commaDelimitedListToStringArray(pipelineConfig.getRiak().getSchema());

    messages.doOnCompleted(cluster::shutdown)
            .doOnError(ex -> {
              errorStream.onNext(ex);
              counters.increment(ERROR_COUNT);
            })
            .subscribe(this);
  }

  @PreDestroy
  public void cleanup() {
    cluster.shutdown();
  }

  @Override
  public void call(String msg) {
    try {
      // Require all values to be strings in the JSON. We'll convert them based on the schema.
      List<String> row = mapper.readValue(msg, new TypeReference<List<String>>() {
      });
      if (LOG.isDebugEnabled()) {
        LOG.debug("Read row {} from message string.", row);
      }

      FastList<Cell> cells = FastList.newList(schema.length);
      if (schema.length != row.size()) {
        throw new IllegalStateException("Row does not conform to schema. Expected "
                                        + schema
                                        + " but got "
                                        + row);
      }
      for (int len = row.size(), i = 0; i < len; i++) {
        switch (schema[i]) {
          case "set":
            throw new IllegalArgumentException("DDL type 'set' not supported");
          case "timestamp":
            cells.add(Cell.newTimestamp(Long.parseLong(row.get(i))));
            continue;
          case "double":
            cells.add(new Cell(Double.parseDouble(row.get(i))));
            continue;
          case "sint64":
            cells.add(new Cell(Long.parseLong(row.get(i))));
            continue;
          case "boolean":
            cells.add(new Cell(Boolean.parseBoolean(row.get(i))));
            continue;
          default:
            cells.add(new Cell(row.get(i)));
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Storing row to Riak bucket {}", pipelineConfig.getRiak().getBucket());
      }
      client.execute(new Store.Builder(pipelineConfig.getRiak().getBucket())
                         .withRow(new Row(cells))
                         .build());

      counters.increment(MSG_COUNT);
      counters.reset(ERROR_COUNT);
    } catch (Exception ex) {
      counters.increment(ERROR_COUNT);
      errorStream.onNext(ex);
    }
  }

}
