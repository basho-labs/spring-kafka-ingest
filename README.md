# Riak/TS Kafka Ingest Microservice

`hachiman-ingest` is a configurable microservice that connects a single Kafka consumer group to a single Riak/TS
bucket type. The app itself is expected to be deployed to Mesos via Marathon and Docker and is parameterizable using
environment variables defined in the deployment JSON file.

## Configuration

The ingester is configured via environment variables and/or system properties set via the `-D` option to the `java` command. The following environment variables can be set to configure how the ingester works:

* `INGEST_GROUP` || `-Dhachiman.ingest.group`=*ingest* - Sets what "group" this instance of the ingester is associated with. Each node that is deployed that uses the same group name will be partitioned with the same consumer group name in the Kafka consumer. If you deploy the ingester via Marathon and specify 5 instances, then there will be 5 different Kafka consumers created, all using the same group name, resulting in a partition of 5 for consuming messages.

* `INGEST_KAFKA_TOPIC` || `-Dhachiman.ingest.kafka.topic`=*ingest* - What Kafka topic whitelist to consume from. This can be a discreet value, like a specific topic name, or a regular expression as discussed in the Kafka consumer API documentation (intro [here](https://cwiki.apache.org/confluence/display/KAFKA/Consumer+API+changes#ConsumerAPIchanges-WhatisaTopicFilter?)).

* `INGEST_KAFKA_ZOOKEEPERS` || `-Dhachiman.ingest.kafka.zookeepers`=*localhost:2181* - Comma-separated HOST:PORT addresses of Zookeeper servers to use to connect to Kafka brokers.

* `INGEST_KAFKA_BROKERS` || `-Dhachiman.ingest.kafka.brokers`=*localhost:9092* - Comma-separated HOST:PORT broker list for publishing messages in unit tests. Not used in production.

* `INGEST_RIAK_BUCKET` || `-Dhachiman.ingest.riak.bucket`=*ingest* - Bucket/table name to insert data into. Must match the value used in the DDL `CREATE TABLE` statement used to create the TS bucket type.

* `INGEST_RIAK_HOSTS` || `-Dhachiman.ingest.riak.hosts`=*localhost:8087* - Comma-separated HOST:PORT values that point to valid Riak TS Protocol Buffers ports.

* `INGEST_RIAK_SCHEMA` || `-Dhachiman.ingest.riak.schema`= - Comma-separated string of type names used in the Riak DDL `CREATE TABLE` command. Should map 1-to-1 between schema type name and value in the incoming JSON string array. Strings will be converted to the correct types before being inserted into a Riak client `Row` ([javadoc](http://basho.github.io/riak-java-client/2.0.3/index.html?com/basho/riak/client/core/query/timeseries/Row.html)), which is a set of `Cell`s ([javadoc](http://basho.github.io/riak-java-client/2.0.3/index.html?com/basho/riak/client/core/query/timeseries/Cell.html)). Possible mappings are:
  - `timestamp` -> `java.lang.Long`
  - `sint64` -> `java.lang.Long`
  - `double` -> `java.lang.Double`
  - `boolean` -> `java.lang.Boolean`

#### Schema Example

A schema of `"timestamp,sint64,double,boolean"` would convert an incoming JSON message like this:

```json
[
  "123456789",
  "123456789",
  "1234.56789",
  "true"
]
```

to this:

```java
new Row(
  Cell.newTimestamp(Long.parseLong("123456789")),
  new Cell(Long.parseLong("123456789")),
  new Cell(Double.parseDouble("1234.56789")),
  new Cell(Boolean.valueOf("true"))
)
```

The ingester would then issue a `Store` ([javadoc](http://basho.github.io/riak-java-client/2.0.3/index.html?com/basho/riak/client/api/commands/timeseries/Store.html)) command using the Riak Java client.

## Deployment

The ingester microservice can be deployed a number of different ways. The build provides a task to create a Docker image which can be pushed a Docker repository (public or private), a "fat" JAR can be created which can be executed via `java -jar`, the Spring Boot application can be run from Gradle via `./gradlew bootRun`, or the `IngestApplication` class can be run via Java main from an IDE.

### Deployment in Mesos

To deploy the ingester to Mesos using Marathon and Docker, modify the sample JSON found in the source code at `src/test/sh/hachiman-ingest.json` to reflect your chosen table and bucket names, as well as provide hostname mappings for all the machines your application might connect to. This should include hosts running Kafka and Riak nodes. If using the sample data found inside the project (`src/test/resources/data/2015.json`), then use the provided `INGEST_RIAK_SCHEMA`. If using your own sample data, then the `INGEST_RIAK_SCHEMA` environment variable should reflect the types used in the `CREATE TABLE` statement.

You can POST the JSON to Marathon via `curl` like so:

    $ curl -XPOST -H "Content-Type: application/json" -d @./src/test/sh/hachiman-ingest.json http://marathon.mesos:8080/v2/apps

The Docker image referenced in the JSON is a snapshot version being built using Travis CI and deployed to a Basho-internal Artifactory Docker repository. It may be you want to build your own Docker image and host it on your own Docker repository or on Docker Hub. If using a private repository, be aware that Docker requires authentication in the form of a `config.json` file in the user's `$HOME/.docker/` directory. To deploy an image from a private repository on Marathon, a tar.gz file of one entry (`~/.docker/config.json`) should be made available to Mesos either by syncing this file to all slaves, or providing it via a secure method accessible in Marathon's `uris` configuration element. Refer to [the Marathon documentation on private Docker repositories](https://mesosphere.github.io/marathon/docs/native-docker-private-registry.html) for more information.

### Deployment via IDE

If you want to change the code of the source project, then just import the project into your IDE of choice and set up a Run and/or Debug configuration to invoke the main method of `com.basho.hachiman.ingest.IngestApplication`. It doesn't take any arguments, but the environment variables documented above should be filled-in with the appropriate values for your setup.

## Ingesting Data

To ingest data into Riak/TS, send a message to the Kafka topic you've configured that is a single JSON array of strings. All values **must** be strings. They will be converted to the correct types based on those specified in the `INGEST_RIAK_SCHEMA`. For example, the test data has a schema of `varchar,varchar,timestamp,double,double,double`. When dumping this data into Kafka via `kafka-console-producer.sh`, each message should be of the format:

    [ "BG1", "TMP", "1420167600000", "51.563752", "0.177891", "12" ]

The ingester's `RxRiakConnector` component will use `Long.parseLong` or `Double.parseDouble` as the case may be to convert the strings to the correct type of `Cell` to insert into Riak.

For convenience, simply `cat` the test data into `kafka-console-producer.sh`:

    $ cat ./src/test/resources/data/2015.json | kafka-console-producer.sh --broker-list broker-0.kafka.mesos:31000 --topic ingest
