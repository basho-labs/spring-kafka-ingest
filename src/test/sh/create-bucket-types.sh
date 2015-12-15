#!/bin/bash

RIAK_HOME=${RIAK_HOME:-.}
echo "RIAK_HOME set to: $RIAK_HOME"
BIN_PATH=$RIAK_HOME/bin
echo "Using bin path: $BIN_PATH"
TABLE_NAME=${RIAK_TABLE:-ingest}
echo "Using bucket type/table name: $TABLE_NAME"

echo "Creating bucket type $TABLE_NAME..."
$BIN_PATH/riak-admin bucket-type create $TABLE_NAME '{"props":{"n_val": 3, "table_def": "create table '$TABLE_NAME' (measurementDate timestamp not null,site varchar not null,latitude double,longitude double,species varchar not null,value double,primary key ((site, species, quantum(measurementDate, 24, h)), site, species, measurementDate))"}}'
echo "Activating bucket type $TABLE_NAME..."
$BIN_PATH/riak-admin bucket-type activate $TABLE_NAME
