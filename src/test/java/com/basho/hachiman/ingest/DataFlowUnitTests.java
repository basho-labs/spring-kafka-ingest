package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.config.PipelineConfigFactory;
import com.basho.hachiman.ingest.kafka.RxKafkaConnector;
import com.basho.hachiman.ingest.riak.RxRiakConnector;
import com.basho.riak.client.api.commands.kv.FetchValue;
import com.basho.riak.client.api.commands.timeseries.Delete;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.RiakFuture;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import com.basho.riak.client.core.query.timeseries.Row;
import com.basho.riak.client.core.util.BinaryValue;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by jbrisbin on 11/20/15.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(classes = IngestApplication.class)
public class DataFlowUnitTests {

  private static final Logger LOG = LoggerFactory.getLogger(DataFlowUnitTests.class);

  @Autowired
  RxKafkaConnector app;

  @Autowired
  PipelineConfigFactory pipelineConfigFactory;

  @Autowired
  RxRiakConnector rxRiakConnector;

  @Value("${hachiman.ingest.kafka.brokers}")
  private String kafkaBrokers;

  /**
   * Test table creation script:
   *
   * create table '$TABLE_NAME' (
   * site varchar not null,
   * species varchar not null,
   * measurementDate timestamp not null,
   * latitude double,
   * longitude double,
   * value double,
   * primary key ((site, species, quantum(measurementDate, 24, h)), site, species, measurementDate))"}}
   *
   **/

  @Before
  public void setup() throws Exception {
    KafkaProducer<String, String> producer = new KafkaProducer<>(createProducerConfig());
    String topic = pipelineConfigFactory.getObject().getKafka().getTopic();
    List<String> lines = Arrays.asList(
            "[ \"BX2\", \"WSPD\", \"1428303600000\", \"51.4906102082147\", \"0.158914493927518\", \"1.4\" ]",
            "[ \"BX2\", \"WSPD\", \"1428307200000\", \"51.4906102082147\", \"0.158914493927518\", \"2.4\" ]",
            "[ \"BX2\", \"WSPD\", \"1428310800000\", \"51.4906102082147\", \"0.158914493927518\", \"2.2\" ]",
            "[ \"NF1\", \"WSPD\", \"1429707600000\", \"50.833312\", \"-1.391525\", \"0.9\" ]",
            "[ \"NF1\", \"WSPD\", \"1429711200000\", \"50.833312\", \"-1.391525\", \"0.8\" ]",
            "[ \"NF1\", \"WSPD\", \"1429714800000\", \"50.833312\", \"-1.391525\", \"0.9\" ]",
            "[ \"RG3\", \"WSPD\", \"1424296800000\", \"51.142082\", \"-0.194181\", \"1.1\" ]",
            "[ \"RG3\", \"WSPD\", \"1424300400000\", \"51.142082\", \"-0.194181\", \"1.5\" ]",
            "[ \"TH4\", \"WDIR\", \"1443247200000\", \"51.5150461674013\", \"-0.00841849265642741\", \"109\" ]",
            "[ \"TH4\", \"WDIR\", \"1443258000000\", \"51.5150461674013\", \"-0.00841849265642741\", \"129\" ]",
            "[ \"TH4\", \"WDIR\", \"1443261600000\", \"51.5150461674013\", \"-0.00841849265642741\", \"139\" ]",
            "[ \"TH4\", \"WDIR\", \"1443265200000\", \"51.5150461674013\", \"-0.00841849265642741\", \"169\" ]"
    );
    for (String message : lines) {
      producer.send(new ProducerRecord<>(topic, message));
    }
  }

  @Test
  public void endToEndDataFlowTest() throws Exception {
    LOG.debug("Waiting for storing test data to riak-ts...");
    Thread.sleep(5000);

    Long to1 = getLatestTimestamp() + 10;
    Long from1 = to1 - 6000;

    String queryText1 = getQuery(from1, to1);
    LOG.debug("Querying data: {}", queryText1);

    Query query1 = new Query.Builder(queryText1).build();
    QueryResult queryResult1 = rxRiakConnector.getRiakClient().execute(query1);

    List<Row> rows = queryResult1.getRows();

    assertEquals(9, queryResult1.getColumnDescriptions().size());
    assertEquals(12, rows.size());

    assertEquals(rows.stream().filter(row -> row.getCells().get(3).getVarcharAsUTF8String().equals("BX2")).count(), 3);
    assertEquals(rows.stream().filter(row -> row.getCells().get(3).getVarcharAsUTF8String().equals("NF1")).count(), 3);
    assertEquals(rows.stream().filter(row -> row.getCells().get(3).getVarcharAsUTF8String().equals("RG3")).count(), 2);
    assertEquals(rows.stream().filter(row -> row.getCells().get(3).getVarcharAsUTF8String().equals("TH4")).count(), 4);

    List<Cell> cells = rows.get(0).getCells();

    assertTrue(cells.get(3).getVarcharAsUTF8String().equals("BX2"));
    assertTrue(cells.get(4).getVarcharAsUTF8String().equals("WSPD"));
    assertTrue(cells.get(5).getTimestamp() == 1428303600000L);
    assertTrue(cells.get(6).getDouble() == 51.4906102082147);
    assertTrue(cells.get(7).getDouble() == 0.158914493927518);
    assertTrue(cells.get(8).getDouble() == 1.4);
  }

  private String getQuery(Long from, Long to) throws Exception {
    return "select * from " + pipelineConfigFactory.getObject().getRiak().getBucket() +
            " where (time > " + from +
            " and time < "+ to + ") and surrogate_key = '1' and family='f'";
  }

  private Long getLatestTimestamp() throws Exception {
    Location location = rxRiakConnector.getKVLocation();
    FetchValue fv = new FetchValue.Builder(location).build();
    FetchValue.Response response = rxRiakConnector.getRiakClient().execute(fv);
    String timestamp = response.getValue(String.class);
    return Long.valueOf(timestamp);
  }

  private Properties createProducerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
    return props;
  }

  @After
  public void clean() throws Exception {
    List<List<Cell>> keys = Arrays.asList(
            Arrays.asList(new Cell("BX2"), new Cell("WSPD"), Cell.newTimestamp(1428303600000L)),
            Arrays.asList(new Cell("BX2"), new Cell("WSPD"), Cell.newTimestamp(1428307200000L)),
            Arrays.asList(new Cell("BX2"), new Cell("WSPD"), Cell.newTimestamp(1428310800000L)),
            Arrays.asList(new Cell("NF1"), new Cell("WSPD"), Cell.newTimestamp(1429707600000L)),
            Arrays.asList(new Cell("NF1"), new Cell("WSPD"), Cell.newTimestamp(1429711200000L)),
            Arrays.asList(new Cell("NF1"), new Cell("WSPD"), Cell.newTimestamp(1429714800000L)),
            Arrays.asList(new Cell("RG3"), new Cell("WSPD"), Cell.newTimestamp(1424296800000L)),
            Arrays.asList(new Cell("RG3"), new Cell("WSPD"), Cell.newTimestamp(1424300400000L)),
            Arrays.asList(new Cell("TH4"), new Cell("WDIR"), Cell.newTimestamp(1443247200000L)),
            Arrays.asList(new Cell("TH4"), new Cell("WDIR"), Cell.newTimestamp(1443265200000L)),
            Arrays.asList(new Cell("TH4"), new Cell("WDIR"), Cell.newTimestamp(1443261600000L)),
            Arrays.asList(new Cell("TH4"), new Cell("WDIR"), Cell.newTimestamp(1443265200000L))
    );
    for (List<Cell> keyCells : keys) {
      Delete delete = new Delete.Builder(pipelineConfigFactory.getObject().getRiak().getBucket(), keyCells).build();
      final RiakFuture<Void, BinaryValue> deleteFuture = rxRiakConnector.getRiakClient().executeAsync(delete);

      deleteFuture.await();
      if (!deleteFuture.isSuccess()) {
        LOG.warn("Deletion failed: {}", deleteFuture.cause());
      }
    }

  }

}
