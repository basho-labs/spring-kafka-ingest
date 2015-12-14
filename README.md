# hachiman-ingest

## Create Riak TS bucket

    # riak-admin bucket-type create ingester_test '{"props":{"n_val": 3, "table_def": "create table ingester_test (measurementDate timestamp not null, site varchar not null, latitude double, longitude double, species varchar not null, value double, primary key ((site, species, quantum(measurementDate, 1, h)), site, species, measurementDate))"}}'
    
    # riak-admin bucket-type activate ingester_test
    
## Configure the ingester

Put pipeline settings into application.yaml:

    hachiman:
      ingest:
        group: ${INGEST_GROUP:default}
        default: 
          config: '{
                     "name": "test-pipeline",
                     "kafka": {"topic": "raw-ingest", "zookeepers": ["localhost:2181"]},
                     "riak": {"bucket": "ingester_test", "hosts": ["172.17.0.2:8087"], "schema":"timestamp,varchar,double,double,varchar,double"}
                   }'

## Start the ingester

    $ ./gradlew bootRun

## Load test data to Kafka topic

You can find example JSON data here: https://github.com/basho-labs/hachiman-ingest/blob/master/src/test/resources/data/2015.json

    $ cat ./data/2015.json | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic raw-ingest
