package com.basho.hachiman.ingest.restapi;

import com.basho.hachiman.ingest.Pipeline;
import com.basho.hachiman.ingest.fn.PipelineToMapUpdate;
import com.basho.hachiman.ingest.fn.RiakMapToPipeline;
import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.api.commands.datatypes.*;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.gs.collections.impl.map.mutable.ConcurrentHashMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.context.request.async.DeferredResult;
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
  @Value("${hachiman.ingest.config.riak.bucket}")
  private String riakBucketName;
  @Value("${hachiman.ingest.config.riak.timeout}")
  private int    riakTimeout;

  private Namespace namespace;

  private final RiakMapToPipeline    rm2p;
  private final PipelineToMapUpdate  p2mu;
  private final RiakClient           client;
  private final Observable<Pipeline> pipelines;

  private final ConcurrentHashMap<String, Pipeline> pipelineCache = ConcurrentHashMap.newMap();

  @Autowired
  public PipelineAdminController(RiakMapToPipeline rm2p,
                                 PipelineToMapUpdate p2mu,
                                 RiakClient client,
                                 Observable<Pipeline> pipelines) {
    this.rm2p = rm2p;
    this.p2mu = p2mu;
    this.client = client;
    this.pipelines = pipelines;
  }

  @PostConstruct
  public void init() {
    this.namespace = new Namespace(ingestGroup, riakBucketName);
    this.pipelines.subscribe(pipeline -> pipelineCache.put(pipeline.getName(), pipeline));
  }

  @RequestMapping(path = "/pipeline", method = RequestMethod.POST, consumes = "application/json")
  public DeferredResult<ResponseEntity<Void>> addPipeline(@RequestBody Pipeline pipeline) {
    return updatePipeline(pipeline.getName(), pipeline);
  }

  @RequestMapping(path = "/pipeline/{name}", method = RequestMethod.PUT, consumes = "application/json")
  public DeferredResult<ResponseEntity<Void>> updatePipeline(@PathVariable String name,
                                                             @RequestBody Pipeline pipeline) {
    DeferredResult<ResponseEntity<Void>> result = new DeferredResult<>();

    MapUpdate upd = p2mu.call(pipeline);
    client.executeAsync(new UpdateMap.Builder(new Location(namespace, name), upd).build())
          .addListener(f -> {
            if (!f.isSuccess()) {
              result.setErrorResult(f.cause());
              return;
            }

            result.setResult(new ResponseEntity<>(HttpStatus.NO_CONTENT));
          });

    return result;
  }

  @RequestMapping(path = "/pipeline/{name}/riak/hosts", method = RequestMethod.POST, consumes = "application/json")
  public DeferredResult<ResponseEntity<Void>> addHosts(@PathVariable String name,
                                                       @RequestBody List<String> hosts) {
    return updateHosts(name, hosts, false);
  }

  @RequestMapping(path = "/pipeline/{name}/riak/hosts", method = RequestMethod.DELETE, consumes = "application/json")
  public DeferredResult<ResponseEntity<Void>> removeHosts(@PathVariable String name,
                                                          @RequestBody List<String> hosts) {
    return updateHosts(name, hosts, true);
  }

  @RequestMapping(path = "/pipeline/{name}")
  public DeferredResult<ResponseEntity<Pipeline>> getPipelineByName(@PathVariable String name) {
    DeferredResult<ResponseEntity<Pipeline>> result = new DeferredResult<>();

    FetchMap fetch = new FetchMap.Builder(new Location(namespace, name))
        .withTimeout(riakTimeout)
        .build();

    client.executeAsync(fetch)
          .addListener(f -> {
            if (!f.isSuccess()) {
              result.setErrorResult(f.cause());
              return;
            }

            result.setResult(new ResponseEntity<>(rm2p.call(f.getNow().getDatatype()), HttpStatus.OK));
          });

    return result;
  }

  private DeferredResult<ResponseEntity<Void>> updateHosts(String name, List<String> hosts, boolean remove) {
    DeferredResult<ResponseEntity<Void>> result = new DeferredResult<>();

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

      client.executeAsync(fetch)
            .addListener(f -> {
              if (!f.isSuccess()) {
                result.setErrorResult(f.cause());
                return;
              }

              Context ctx = f.getNow().getContext();
              try {
                client.execute(new UpdateMap.Builder(location, upd)
                                   .withContext(ctx)
                                   .build());

                result.setResult(new ResponseEntity<>(HttpStatus.NO_CONTENT));
              } catch (ExecutionException | InterruptedException e) {
                result.setErrorResult(e.getCause());
              }
            });
    } else {
      client.executeAsync(new UpdateMap.Builder(new Location(namespace, name), upd).build())
            .addListener(f -> {
              if (!f.isSuccess()) {
                result.setErrorResult(f.cause());
                return;
              }

              result.setResult(new ResponseEntity<>(HttpStatus.NO_CONTENT));
            });
    }

    return result;
  }

}
