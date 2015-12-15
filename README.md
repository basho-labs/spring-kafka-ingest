# Riak/TS Kafka Ingest Microservice

`hachiman-ingest` is a configurable microservice that connects a single Kafka consumer group to a single Riak/TS
bucket type. The app itself is expected to be deployed to Mesos via Marathon and Docker and is parameterizable using
environment variables defined in the deployment JSON file.

## Configuration

The ingester is configured via environment variables and/or system properties set via the `-D` option to the `java` command. The following environment variables can be set to configure how the ingester works:

* `INGEST_GROUP` || `-Dhachiman.ingest.group`=*ingest* - Sets what "group" this instance of the ingester is associated with. Each node that is deployed that uses the same group name will be partitioned with the same consumer group name in the Kafka consumer. If you deploy the ingester via Marathon and specify 5 instances, then there will be 5 different Kafka consumers created, all using the same group name, resulting in a partition of 5 for consuming messages.

* `INGEST_KAFKA_TOPIC` || `-Dhachiman.ingest.kafka.topic`=*ingest* - What Kafka topic whitelist to consume from. This can be a discreet value, like a specific topic name, or a regular expression as discussed in the Kafka consumer API documentation (intro [here](https://cwiki.apache.org/confluence/display/KAFKA/Consumer+API+changes#ConsumerAPIchanges-WhatisaTopicFilter?)).
* `INGEST_KAFKA_ZOOKEEPERS` (default: 'localhost:2181') Comma-separated HOST:PORT addresses of Zookeeper servers to use to connect to Kafka brokers.
* `INGEST_KAFKA_BROKERS` (default: 'localhost:9092') Comma-separated HOST:PORT broker list for publishing messages in unit tests. Not used in production.
* `INGEST_RIAK_BUCKET` (default: 'ingest') Bucket/table name to insert data into. Must match the value used in the DDL `CREATE TABLE` statement used to create the TS bucket type.
* `INGEST_RIAK_HOSTS` (default: 'localhost:8087') Comma-separated HOST:PORT values that point to valid Riak TS Protocol Buffers ports.
* `INGEST_RIAK_SCHEMA` (default: none) Comma-separated string of type names used in the Riak DDL `CREATE TABLE` command. Should correspond to values coming in via JSON string arrays through Kafka. Schema types will be cached when the application is started and values that appear as strings in the JSON array will be converted to the correct types before being inserted into a Riak client `Row` ([javadoc](http://basho.github.io/riak-java-client/2.0.3/index.html?com/basho/riak/client/core/query/timeseries/Row.html)), which is a set of `Cell`s ([javadoc](http://basho.github.io/riak-java-client/2.0.3/index.html?com/basho/riak/client/core/query/timeseries/Cell.html)).

## Start the ingester

    $ ./gradlew bootRun

## Load test data to Kafka topic

You can find example JSON data here: https://github.com/basho-labs/hachiman-ingest/blob/master/src/test/resources/data/2015.json

    $ cat ./data/2015.json | /opt/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic ingest
