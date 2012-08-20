package org.apache.lucene.search;

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

import org.apache.lucene.util.RamUsageEstimator;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;

/**
 * Interface that fronts the caching of filters and their underlying DocSets such that filters can be
 * reused inside lucene
 */
public interface FilterCache {

  /**
   * @param readerKey the {@link org.apache.lucene.index.AtomicReader#getCoreCacheKey()} that represents the reader
   *                  for the purposes of caching
   * @param filterKey the {@link Filter} that is used as a second level cache key to identify previously cached
   *                  filters
   * @return DocIdSet present in the cache, null if no DocIdSet is present
   */
  public DocIdSet get(Object readerKey, Filter filterKey);

  /**
   * @param readerKey the {@link org.apache.lucene.index.AtomicReader#getCoreCacheKey()} that represents the reader
   *                  for the purposes of caching
   * @param filterKey the {@link Filter} that is used as a second level cache key to identify previously cached
   *                  filters
   * @param toCache   the {@link DocIdSet} that will be cached on behalf of this filter
   */
  public void put(Object readerKey, Filter filterKey, DocIdSet toCache);

  /** Returns total byte size used by cached filters. */
  public long sizeInBytes();

  /**
   * Simplistic implementation of a FilterCache that is designed for clients that want to use CachingWrappingFilter without
   * any requirement to create their own complex cache implementations.
   *
   * This is backed on a two-level WeakHashMap with simple concurrency wrapping and may not be the best performing
   * cache implementation available
   */
  public final class SimpleCache implements FilterCache {

    private final Map<Object, Map<Filter, DocIdSet>> cache =  new WeakHashMap<Object, Map<Filter, DocIdSet>>();

    @Override
    public DocIdSet get(Object readerKey, Filter filterKey) {
      synchronized (cache) {
        if (cache.containsKey(readerKey)) {
          return cache.get(readerKey).get(filterKey);
        }
      }
      return null;
    }

    @Override
    public void put(Object readerKey, Filter filterKey, DocIdSet toCache) {
      synchronized (cache) {
        if (!cache.containsKey(readerKey)) {
          cache.put(readerKey, new HashMap<Filter, DocIdSet>());
        }

        cache.get(readerKey).put(filterKey, toCache);
      }
    }

    /** Returns total byte size used by cached filters. */
    public long sizeInBytes() {
      // Sync only to pull the current set of values:
      long total = 0;
      synchronized(cache) {
        for (Map<Filter, DocIdSet> maps : cache.values()) {
          for (DocIdSet docSet : maps.values()) {
            total += RamUsageEstimator.sizeOf(docSet);
          }
        }
      }

      return total;
    }
  }

}
