package org.apache.solr.store.blockcache;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.net.URL;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.lucene.store.blockcache.Metrics;
import org.apache.solr.core.SolrInfoMBean;
import org.apache.solr.search.SolrCacheBase;

import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.core.SolrCore;

/**
 * Extension of the metrics interface that allows for publication of metrics in solr
 */
public class SolrMetrics extends Metrics implements SolrInfoMBean {

  private long previous = System.nanoTime();

  public NamedList<Number> getStatistics() {
    NamedList<Number> stats = new SimpleOrderedMap<>(21); // room for one method call before growing

    long now = System.nanoTime();
    float seconds = (now - previous) / 1000000000.0f;

    long hits = blockCacheHit.getAndSet(0);
    long lookups = hits + blockCacheMiss.getAndSet(0);

    stats.add("lookups", getPerSecond(lookups, seconds));
    stats.add("hits", getPerSecond(hits, seconds));
    stats.add("hitratio", SolrCacheBase.calcHitRatio(lookups, hits));
    stats.add("evictions", getPerSecond(blockCacheEviction.getAndSet(0), seconds));
    stats.add("size", blockCacheSize.get());
    stats.add("row.reads", getPerSecond(rowReads.getAndSet(0), seconds));
    stats.add("row.writes", getPerSecond(rowWrites.getAndSet(0), seconds));
    stats.add("record.reads", getPerSecond(recordReads.getAndSet(0), seconds));
    stats.add("record.writes", getPerSecond(recordWrites.getAndSet(0), seconds));
    stats.add("query.external", getPerSecond(queriesExternal.getAndSet(0), seconds));
    stats.add("query.internal", getPerSecond(queriesInternal.getAndSet(0), seconds));
    stats.add("buffercache.allocations", getPerSecond(shardBuffercacheAllocate.getAndSet(0), seconds));
    stats.add("buffercache.lost", getPerSecond(shardBuffercacheLost.getAndSet(0), seconds));

    for (Map.Entry<String, MethodCall> entry : methodCalls.entrySet()) {
      String key = entry.getKey();
      MethodCall value = entry.getValue();
      long invokes = value.invokes.getAndSet(0);
      long times = value.times.getAndSet(0);

      float avgTimes = (times / (float) invokes) / 1000000000.0f;
      stats.add("methodcalls." + key + ".count", getPerSecond(invokes, seconds));
      stats.add("methodcalls." + key + ".time", avgTimes);
    }
    stats.add("tables", tableCount.get());
    stats.add("rows", rowCount.get());
    stats.add("records", recordCount.get());
    stats.add("index.count", indexCount.get());
    stats.add("index.memoryusage", indexMemoryUsage.get());
    stats.add("index.segments", segmentCount.get());
    previous = now;

    return stats;
  }

  private float getPerSecond(long value, float seconds) {
    return (float) (value / seconds);
  }

  // SolrInfoMBean methods

  @Override
  public String getName() {
    return "HdfsBlockCache";
  }

  @Override
  public String getDescription() {
    return "Provides metrics for the HdfsDirectoryFactory BlockCache.";
  }

  @Override
  public String getSource() {
    return null;
  }

  @Override
  public URL[] getDocs() {
    return null;
  }

  @Override
  public String getVersion() {
    return SolrCore.version;
  }

  @Override
  public Category getCategory() {
    return Category.CACHE;
  }
}
