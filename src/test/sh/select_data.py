from riak import RiakClient, RiakNode, Table

cl = RiakClient(host='riakts', pb_port=8087)
tbl = Table(cl, 'ingest')
for chunk in tbl.stream_keys():
  for key in chunk:
    print key
