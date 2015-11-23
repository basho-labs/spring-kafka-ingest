package com.basho.hachiman.ingest.config;

import java.util.List;

/**
 * Created by jbrisbin on 11/20/15.
 */
public class RiakConfig {

  private String       bucket;
  private List<String> hosts;

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public List<String> getHosts() {
    return hosts;
  }

  public void setHosts(List<String> hosts) {
    this.hosts = hosts;
  }

}
