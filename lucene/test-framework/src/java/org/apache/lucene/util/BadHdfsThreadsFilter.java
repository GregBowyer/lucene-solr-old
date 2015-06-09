package org.apache.lucene.util;

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

import java.util.HashSet;
import java.util.Set;

import com.carrotsearch.randomizedtesting.ThreadFilter;

public class BadHdfsThreadsFilter implements ThreadFilter {

  private static final Set<String> badThreads = new HashSet<>();
  static {
    badThreads.add("IPC Parameter Sending Thread "); // SOLR-5007
    badThreads.add("org.apache.hadoop.hdfs.PeerCache"); // SOLR-7288)
    badThreads.add("LeaseRenewer"); // SOLR-7287
    badThreads.add("Acceptor");
    badThreads.add("Timer");
    badThreads.add("pool");
    badThreads.add("qtp");
  }

  @Override
  public boolean reject(Thread t) {
    String name = t.getName();
    for (String badThread : badThreads) {
      if (name.contains(badThread)) {
        return true;
      }
    }
    return false;
  }
}
