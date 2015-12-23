#!/bin/bash

RIAK_HOME=${RIAK_HOME:-.}
echo "RIAK_HOME set to: $RIAK_HOME"
BIN_PATH=$RIAK_HOME/bin
echo "Using bin path: $BIN_PATH"
TABLE_NAME=${RIAK_TABLE:-ingest}
echo "Using bucket type/table name: $TABLE_NAME"
KV_TABLE_NAME=${RIAK_TABLE:-ingest-kv}

echo "Creating bucket type $TABLE_NAME..."
$BIN_PATH/riak-admin bucket-type create $TABLE_NAME '{"props":{"n_val": 3, "table_def": "create table '$TABLE_NAME' (surrogate_key varchar not null, family varchar not null, time timestamp not null, site varchar not null, species varchar not null, measurementDate timestamp not null,latitude double,longitude double,value double, primary key ((surrogate_key, family, quantum(time, 24, ''h'')), surrogate_key, family, time))"}}'
echo "Activating bucket type $TABLE_NAME..."
$BIN_PATH/riak-admin bucket-type activate $TABLE_NAME

echo "Creating bucket type $KV_TABLE_NAME..."
$BIN_PATH/riak-admin bucket-type create $KV_TABLE_NAME '{"props":{"allow_mult":false}}'
echo "Activating bucket type $KV_TABLE_NAME..."
$BIN_PATH/riak-admin bucket-type activate $KV_TABLE_NAME