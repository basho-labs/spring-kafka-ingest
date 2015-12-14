#!/bin/bash

BIN_PATH=${RIAK_HOME:-.}/bin
TABLE_NAME=${RIAK_TABLE:-ingest}

$BIN_PATH/riak-admin bucket-type create $TABLE_NAME '{"props":{"n_val": 3, "table_def": "create table '$TABLE_NAME' (measurementDate timestamp not null,site varchar not null,latitude double,longitude double,species varchar not null,value double,primary key ((site, species, quantum(measurementDate, 24, h)), site, species, measurementDate))"}}'