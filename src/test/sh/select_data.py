from riak import RiakClient, RiakNode, Table

cl = RiakClient(host='mesos-4', pb_port=31998)
keys = cl.ts_stream_keys(Table(cl, 'ingest'))
for chunk in keys:
    for key in chunk:
        print key
