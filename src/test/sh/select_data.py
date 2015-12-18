from riak import RiakClient, RiakNode, Table

cl = RiakClient(host='mesos-4', pb_port=31998)
tbl = Table(cl, 'ingest')
for chunk in tbl.stream_keys():
  for key in chunk:
    print key
