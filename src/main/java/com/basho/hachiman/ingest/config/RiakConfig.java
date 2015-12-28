package com.basho.hachiman.ingest.config;

import java.util.Set;

/**
 * Domain object that encapsulates the configuration necessary to connect to Riak.
 */
public class RiakConfig {

  private String      bucket;
  private String      surrogateKey;
  private String      surrogateKeyValue;
  private String      surrogateFamily;
  private String      surrogateFamilyValue;
  private String      offsetKey;
  private String      kvBucket;
  private String      kvKey;
  private Set<String> hosts;
  private String      schema;

  public RiakConfig() {
  }

  public RiakConfig(String bucket, String surrogateKey, String surrogateKeyValue, String surrogateFamily, String surrogateFamilyValue, String offsetKey, String kvBucket, String kvKey, Set<String> hosts, String schema) {
    super();
    this.bucket = bucket;
    this.surrogateKey = surrogateKey;
    this.surrogateKeyValue = surrogateKeyValue;
    this.surrogateFamily = surrogateFamily;
    this.surrogateFamilyValue = surrogateFamilyValue;
    this.offsetKey = offsetKey;
    this.kvBucket = kvBucket;
    this.kvKey = kvKey;
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

  public String getSurrogateKey() {
    return surrogateKey;
  }

  public RiakConfig setSurrogateKey(String surrogateKey) {
    this.surrogateKey = surrogateKey;
    return this;
  }

  public String getSurrogateKeyValue() {
    return surrogateKeyValue;
  }

  public RiakConfig setSurrogateKeyValue(String surrogateKeyValue) {
    this.surrogateKeyValue = surrogateKeyValue;
    return this;
  }

  public String getSurrogateFamily() {
    return surrogateFamily;
  }

  public RiakConfig setSurrogateFamily(String surrogateFamily) {
    this.surrogateFamily = surrogateFamily;
    return this;
  }

  public String getSurrogateFamilyValue() {
    return surrogateFamilyValue;
  }

  public RiakConfig setSurrogateFamilyValue(String surrogateFamilyValue) {
    this.surrogateFamilyValue = surrogateFamilyValue;
    return this;
  }

  public String getOffsetKey() {
    return offsetKey;
  }

  public RiakConfig setOffsetKey(String offsetKey) {
    this.offsetKey = offsetKey;
    return this;
  }

  public String getKvBucket() {
    return kvBucket;
  }

  public RiakConfig setKvBucket(String kvBucket) {
    this.kvBucket = kvBucket;
    return this;
  }

  public String getKvKey() {
    return kvKey;
  }

  public RiakConfig setKvKey(String kvKey) {
    this.kvKey = kvKey;
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
           "bucket='" + bucket  + '\'' +
           ", surrogateKey='" + surrogateKey + '\'' +
           ", surrogateKeyValue='" + surrogateKeyValue + '\'' +
           ", surrogateFamily='" + surrogateFamily + '\'' +
           ", surrogateFamilyValue='" + surrogateFamilyValue + '\'' +
           ", offsetKey='" + offsetKey + '\'' +
           ", kvBucket='" + kvBucket + '\'' +
           ", kvKey='" + kvKey + '\'' +
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
    if (surrogateKey != null ? !surrogateKey.equals(config.surrogateKey) : config.surrogateKey != null) return false;
    if (surrogateKeyValue != null ? !surrogateKeyValue.equals(config.surrogateKeyValue) : config.surrogateKeyValue != null) return false;
    if (surrogateFamily != null ? !surrogateFamily.equals(config.surrogateFamily) : config.surrogateFamily != null) return false;
    if (surrogateFamilyValue != null ? !surrogateFamilyValue.equals(config.surrogateFamilyValue) : config.surrogateFamilyValue != null) return false;
    if (offsetKey != null ? !offsetKey.equals(config.offsetKey) : config.offsetKey != null) return false;
    if (kvBucket != null ? !kvBucket.equals(config.kvBucket) : config.kvBucket != null) return false;
    if (kvKey != null ? !kvKey.equals(config.kvKey) : config.kvKey != null) return false;
    if (hosts != null ? !hosts.equals(config.hosts) : config.hosts != null) return false;
    return !(schema != null ? !schema.equals(config.schema) : config.schema != null);

  }

  @Override
  public int hashCode() {
    int result = bucket != null ? bucket.hashCode() : 0;
    result = 31 * result + (surrogateKey != null ? surrogateKey.hashCode() : 0);
    result = 31 * result + (surrogateKeyValue != null ? surrogateKeyValue.hashCode() : 0);
    result = 31 * result + (surrogateFamily != null ? surrogateFamily.hashCode() : 0);
    result = 31 * result + (surrogateFamilyValue != null ? surrogateFamilyValue.hashCode() : 0);
    result = 31 * result + (offsetKey != null ? offsetKey.hashCode() : 0);
    result = 31 * result + (kvBucket != null ? kvBucket.hashCode() : 0);
    result = 31 * result + (kvKey != null ? kvKey.hashCode() : 0);
    result = 31 * result + (hosts != null ? hosts.hashCode() : 0);
    result = 31 * result + (schema != null ? schema.hashCode() : 0);
    return result;
  }

}
