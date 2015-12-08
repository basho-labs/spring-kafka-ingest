package com.basho.hachiman.ingest.config;

import java.util.Set;

/**
 * Created by jbrisbin on 11/20/15.
 */
public class RiakConfig {

  private String      bucket;
  private Set<String> hosts;
  private String      schema;

  public RiakConfig() {
  }

  public RiakConfig(String bucket, Set<String> hosts, String schema) {
    super();
    this.bucket = bucket;
    this.hosts = hosts;
    this.schema = schema;
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

  public String getSchema() {
    return schema;
  }

  public RiakConfig setSchema(String schema) {
    this.schema = schema;
    return this;
  }

  @Override
  public String toString() {
    return "RiakConfig{" +
           "bucket='" + bucket + '\'' +
           ", hosts=" + hosts +
           ", schema='" + schema + '\'' +
           '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    RiakConfig config = (RiakConfig) o;

    if (bucket != null ? !bucket.equals(config.bucket) : config.bucket != null) return false;
    if (hosts != null ? !hosts.equals(config.hosts) : config.hosts != null) return false;
    return !(schema != null ? !schema.equals(config.schema) : config.schema != null);

  }

  @Override
  public int hashCode() {
    int result = bucket != null ? bucket.hashCode() : 0;
    result = 31 * result + (hosts != null ? hosts.hashCode() : 0);
    result = 31 * result + (schema != null ? schema.hashCode() : 0);
    return result;
  }

}
