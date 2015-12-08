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
import org.springframework.stereotype.Component;
import rx.Observable;
import rx.functions.Action1;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.springframework.util.StringUtils.commaDelimitedListToStringArray;

/**
 * Created by jbrisbin on 12/7/15.
 */
@Component
public class RxRiakConnector implements Action1<String> {

  private static final Logger LOG = LoggerFactory.getLogger(RxRiakConnector.class);

  private List<String> schema;
  private RiakCluster  cluster;
  private RiakClient   client;

  private final ObjectMapper       mapper;
  private final PipelineConfig     pipelineConfig;
  private final Observable<String> messages;

  private final AtomicInteger errorCount = new AtomicInteger();

  @Autowired
  public RxRiakConnector(ObjectMapper mapper,
                         PipelineConfig pipelineConfig,
                         RxKafkaConnector kafkaConnector) {
    this.mapper = mapper;
    this.pipelineConfig = pipelineConfig;
    this.messages = kafkaConnector.get();
  }

  @PostConstruct
  public void init() {
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

    schema = FastList.newListWith(commaDelimitedListToStringArray(pipelineConfig.getRiak().getSchema()));

    messages.doOnCompleted(cluster::shutdown)
            .doOnError(ex -> {
              LOG.error(ex.getMessage(), ex);
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
      List<String> row = mapper.readValue(msg, new TypeReference<List<String>>() {
      });

      FastList<Cell> cells = FastList.newList(schema.size());
      if (schema.size() != row.size()) {
        throw new IllegalStateException("Row does not conform to schema. Expected "
                                        + schema
                                        + " but got "
                                        + row);
      }
      for (int len = row.size(), i = 0; i < len; i++) {
        switch (schema.get(i)) {
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

      client.execute(new Store.Builder(pipelineConfig.getRiak().getBucket())
                         .withRow(new Row(cells))
                         .build());

      if (errorCount.get() > 0) {
        errorCount.decrementAndGet();
      }
    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
    }
  }

}
