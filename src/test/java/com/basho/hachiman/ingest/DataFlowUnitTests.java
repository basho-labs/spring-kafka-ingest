package com.basho.hachiman.ingest;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.config.PipelineConfigFactory;
import com.basho.hachiman.ingest.kafka.RxKafkaConnector;
import com.basho.hachiman.ingest.riak.RxRiakConnector;
import com.basho.riak.client.api.commands.timeseries.Query;
import com.basho.riak.client.core.query.timeseries.Cell;
import com.basho.riak.client.core.query.timeseries.QueryResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
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

  private PipelineConfig pipelineConfig;

  @Before
  public void setup() throws Exception {
    this.pipelineConfig = pipelineConfigFactory.getObject();
    KafkaProducer<String, String> producer = new KafkaProducer<>(createProducerConfig());
    String topic = pipelineConfig.getKafka().getTopic();
    List<String> lines = Files.readAllLines(Paths.get(new ClassPathResource("/data/2015.json")
                                                          .getURI()));
    for (String message : lines) {
      producer.send(new ProducerRecord<>(topic, message));
    }
  }

  @Test
  public void endToEndDataFlowTest() throws ExecutionException, InterruptedException {
    LOG.debug("Waiting for storing test data to riak-ts...");
    //Thread.sleep(30000);

    LocalDateTime from1 = LocalDateTime.of(2015, Month.APRIL, 6, 0, 0);
    LocalDateTime to1 = from1.plusDays(3);

    String site1 = "BX2";
    String species1 = "WSPD";

    String queryText1 = getQuery(from1, to1, site1, species1);
    LOG.debug("Querying data: {}", queryText1);

    Query query1 = new Query.Builder(queryText1).build();
    QueryResult queryResult1 = rxRiakConnector.getRiakClient().execute(query1);

    assertEquals(6, queryResult1.getColumnDescriptions().size());
    assertEquals(8, queryResult1.getRows().size());

    LocalDateTime from2 = LocalDateTime.of(2015, Month.APRIL, 22, 1, 0);
    LocalDateTime to2 = from2.plusDays(4);

    String site2 = "NF1";
    String queryText2 = getQuery(from2, to2, site2, species1);
    LOG.debug("Querying data: {}", queryText2);

    Query query2 = new Query.Builder(queryText2).build();
    QueryResult queryResult2 = rxRiakConnector.getRiakClient().execute(query2);

    assertEquals(84, queryResult2.getRows().size());

    String site3 = "RG3";
    LocalDateTime from3 = LocalDateTime.of(2015, Month.FEBRUARY, 18, 1, 0);
    LocalDateTime to3 = from3.plusDays(4);

    String queryText3 = getQuery(from3, to3, site3, species1);
    LOG.debug("Querying data: {}", queryText3);

    Query query3 = new Query.Builder(queryText3).build();
    QueryResult queryResult3 = rxRiakConnector.getRiakClient().execute(query3);

    assertEquals(56, queryResult3.getRows().size());

    String site4 = "TH4";
    String species4 = "WDIR";
    LocalDateTime from4 = LocalDateTime.of(2015, Month.SEPTEMBER, 26, 1, 0);
    LocalDateTime to4 = from4.plusDays(4);

    String queryText4 = getQuery(from4, to4, site4, species4);
    LOG.debug("Querying data: {}", queryText4);

    Query query4 = new Query.Builder(queryText4).build();
    QueryResult queryResult4 = rxRiakConnector.getRiakClient().execute(query4);

    assertEquals(88, queryResult4.getRows().size());
    List<Cell> cells = queryResult4.getRows().get(0).getCells();

    assertTrue(cells.get(0).getTimestamp() == 1443247200000L);
    assertTrue(cells.get(1).getVarcharAsUTF8String().equals("TH4"));
    assertTrue(cells.get(2).getDouble() == 51.5150461674013D);
    assertTrue(cells.get(3).getDouble() == -0.00841849265642741);
    assertTrue(cells.get(4).getVarcharAsUTF8String().equals("WDIR"));
    assertTrue(cells.get(5).getDouble() == 109);
  }

  private String getQuery(LocalDateTime from, LocalDateTime to, String site, String species) {
    ZoneId zoneId = ZoneId.of("UTC");
    return "select * from " + pipelineConfig.getRiak().getBucket() +
            " where measurementDate > " + from.atZone(zoneId).toInstant().toEpochMilli() +
            " and measurementDate < "+ to.atZone(zoneId).toInstant().toEpochMilli() +
            " and site = '" + site + "' and species = '" + species + "'";
  }

  private Properties createProducerConfig() {
    Properties props = new Properties();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBrokers);
    return props;
  }

}
