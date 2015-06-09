package org.apache.lucene.store.blockcache;

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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple class that provides metrics on block cache operations.
 * @lucene.experimental
 */
public class Metrics {
  
  public static class MethodCall {
    public AtomicLong invokes = new AtomicLong();
    public AtomicLong times = new AtomicLong();
  }

  public AtomicLong blockCacheHit = new AtomicLong(0);
  public AtomicLong blockCacheMiss = new AtomicLong(0);
  public AtomicLong blockCacheEviction = new AtomicLong(0);
  public AtomicLong blockCacheSize = new AtomicLong(0);
  public AtomicLong rowReads = new AtomicLong(0);
  public AtomicLong rowWrites = new AtomicLong(0);
  public AtomicLong recordReads = new AtomicLong(0);
  public AtomicLong recordWrites = new AtomicLong(0);
  public AtomicLong queriesExternal = new AtomicLong(0);
  public AtomicLong queriesInternal = new AtomicLong(0);
  public AtomicLong shardBuffercacheAllocate = new AtomicLong(0);
  public AtomicLong shardBuffercacheLost = new AtomicLong(0);
  public Map<String, MethodCall> methodCalls = new ConcurrentHashMap<>();
  
  public AtomicLong tableCount = new AtomicLong(0);
  public AtomicLong rowCount = new AtomicLong(0);
  public AtomicLong recordCount = new AtomicLong(0);
  public AtomicLong indexCount = new AtomicLong(0);
  public AtomicLong indexMemoryUsage = new AtomicLong(0);
  public AtomicLong segmentCount = new AtomicLong(0);

  public static void main(String[] args) throws InterruptedException {
    Metrics metrics = new Metrics();
    MethodCall methodCall = new MethodCall();
    metrics.methodCalls.put("test", methodCall);
    for (int i = 0; i < 100; i++) {
      metrics.blockCacheHit.incrementAndGet();
      metrics.blockCacheMiss.incrementAndGet();
      methodCall.invokes.incrementAndGet();
      methodCall.times.addAndGet(56000000);
      Thread.sleep(500);
    }
  }
}
