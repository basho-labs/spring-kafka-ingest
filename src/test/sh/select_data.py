from riak import RiakClient, RiakNode, Table

cl = RiakClient(host='hachiman', pb_port=10017)
tbl = Table(cl, 'ingest')
for chunk in tbl.stream_keys():
  for key in chunk:
    print key
