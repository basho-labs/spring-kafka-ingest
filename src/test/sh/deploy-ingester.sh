#!/usr/bin/env bash

curl -v -XPOST http://mesos-1.riaktsdemo.net:8080/v2/apps -d @./hachiman-ingest.json -H "Content-Type: application/json"
