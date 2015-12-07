package com.basho.hachiman.ingest.web;

import com.basho.hachiman.ingest.config.PipelineConfig;
import com.basho.hachiman.ingest.fn.PipelineToMapUpdate;
import com.basho.hachiman.ingest.fn.RiakMapToPipeline;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.*;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.crdt.types.RiakMap;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import rx.Observable;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by jbrisbin on 12/1/15.
 */
@RestController
public class PipelineAdminController {

  @Value("${hachiman.ingest.group}")
  private String ingestGroup;
  @Value("${hachiman.ingest.config.riak.timeout}")
  private int    riakTimeout;

  private Namespace namespace;

  private final RiakMapToPipeline          rm2p;
  private final PipelineToMapUpdate        p2mu;
  private final RiakClient                 client;
  private final Observable<PipelineConfig> pipelines;

  private final ConcurrentHashMap<String, PipelineConfig> pipelineCache = ConcurrentHashMap.newMap();

  @Autowired
  public PipelineAdminController(RiakMapToPipeline rm2p,
                                 PipelineToMapUpdate p2mu,
                                 RiakClient client,
                                 Observable<PipelineConfig> pipelines) {
    this.rm2p = rm2p;
    this.p2mu = p2mu;
    this.client = client;
    this.pipelines = pipelines;
  }

  @PostConstruct
  public void init() {
    this.namespace = new Namespace(ingestGroup, ingestGroup);
    this.pipelines.subscribe(pipeline -> pipelineCache.put(pipeline.getName(), pipeline));
  }

  @RequestMapping(path = "/pipeline", method = RequestMethod.POST, consumes = "application/json")
  public ResponseEntity<Void> addPipeline(@RequestBody PipelineConfig pipelineConfig) throws Exception {
    return updatePipeline(pipelineConfig.getName(), pipelineConfig);
  }

  @RequestMapping(path = "/pipeline/{name}", method = RequestMethod.PUT, consumes = "application/json")
  public ResponseEntity<Void> updatePipeline(@PathVariable String name,
                                             @RequestBody PipelineConfig pipelineConfig) throws Exception {
    client.execute(new UpdateMap.Builder(new Location(namespace, name), p2mu.call(pipelineConfig))
                       .withTimeout(riakTimeout)
                       .build());
    return new ResponseEntity<>(HttpStatus.NO_CONTENT);
  }

  @RequestMapping(path = "/pipeline/{name}/riak/hosts", method = RequestMethod.POST, consumes = "application/json")
  public ResponseEntity<Void> addHosts(@PathVariable String name,
                                       @RequestBody List<String> hosts) throws Exception {
    return updateHosts(name, hosts, false);
  }

  @RequestMapping(path = "/pipeline/{name}/riak/hosts", method = RequestMethod.DELETE, consumes = "application/json")
  public ResponseEntity<Void> removeHosts(@PathVariable String name,
                                          @RequestBody List<String> hosts) throws Exception {
    return updateHosts(name, hosts, true);
  }

  @RequestMapping(path = "/pipeline/{name}")
  public ResponseEntity<PipelineConfig> getPipelineByName(@PathVariable String name) throws Exception {
    RiakMap m = client.execute(new FetchMap.Builder(new Location(namespace, name))
                                   .withTimeout(riakTimeout)
                                   .build())
                      .getDatatype();
    return new ResponseEntity<>(rm2p.call(m), HttpStatus.OK);
  }

  private ResponseEntity<Void> updateHosts(String name,
                                           List<String> hosts,
                                           boolean remove) throws Exception {
    MapUpdate upd           = new MapUpdate();
    MapUpdate riakConfigUpd = new MapUpdate();
    SetUpdate hostsUpd      = new SetUpdate();
    hosts.forEach(s -> {
      if (remove) {
        hostsUpd.remove(s);
      } else {
        hostsUpd.add(s);
      }
    });
    riakConfigUpd.update("hosts", hostsUpd);
    upd.update("riak", riakConfigUpd);

    Location location = new Location(namespace, name);
    if (remove) {
      FetchMap fetch = new FetchMap.Builder(location)
          .withTimeout(riakTimeout)
          .build();

      FetchMap.Response resp = client.execute(fetch);
      Context           ctx  = resp.getContext();
      try {
        client.execute(new UpdateMap.Builder(location, upd)
                           .withTimeout(riakTimeout)
                           .withContext(ctx)
                           .build());
        return new ResponseEntity<>(HttpStatus.NO_CONTENT);
      } catch (ExecutionException | InterruptedException e) {
        throw new IllegalStateException(e);
      }
    } else {
      client.execute(new UpdateMap.Builder(new Location(namespace, name), upd)
                         .withTimeout(riakTimeout)
                         .build());
      return new ResponseEntity<>(HttpStatus.NO_CONTENT);
    }
  }

}
