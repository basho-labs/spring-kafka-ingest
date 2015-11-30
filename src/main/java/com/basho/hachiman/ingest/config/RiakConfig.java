package com.basho.hachiman.ingest.config;

import java.util.Set;

/**
 * Created by jbrisbin on 11/20/15.
 */
public class RiakConfig {

  private String      bucket;
  private Set<String> hosts;

  public RiakConfig() {
  }

  public RiakConfig(String bucket, Set<String> hosts) {
    super();
    this.bucket = bucket;
    this.hosts = hosts;
  }

  public String getBucket() {
    return bucket;
  }

  public RiakConfig setBucket(String bucket) {
    this.bucket = bucket;
    return this;
  }

  public Set<String> getHosts() {
    return hosts;
  }

  public RiakConfig setHosts(Set<String> hosts) {
    this.hosts = hosts;
    return this;
  }

  @Override
  public String toString() {
    return "RiakConfig [bucket=" + bucket + ", hosts=" + hosts + "]";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RiakConfig that = (RiakConfig) o;

    if (bucket != null ? !bucket.equals(that.bucket) : that.bucket != null) return false;
    return !(hosts != null ? !hosts.equals(that.hosts) : that.hosts != null);
  }

  @Override
  public int hashCode() {
    int result = bucket != null ? bucket.hashCode() : 0;
    result = 31 * result + (hosts != null ? hosts.hashCode() : 0);
    return result;
  }

}
