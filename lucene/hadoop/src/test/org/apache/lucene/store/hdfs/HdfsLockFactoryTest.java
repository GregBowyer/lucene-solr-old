package org.apache.lucene.store.hdfs;

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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BadHdfsThreadsFilter;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestRuleLimitSysouts;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

@ThreadLeakFilters(defaultFilters = true, filters = {
    BadHdfsThreadsFilter.class // hdfs currently leaks thread(s)
})
@TestRuleLimitSysouts.Limit(bytes = 32 * 1024)
public class HdfsLockFactoryTest extends LuceneTestCase {

  private static MiniDFSCluster dfsCluster;

  @BeforeClass
  public static void beforeClass() throws Exception {
    dfsCluster = HdfsTestUtil.setupClass(LuceneTestCase.createTempDir().toFile().getAbsolutePath());
  }

  @AfterClass
  public static void afterClass() throws Exception {
    HdfsTestUtil.teardownClass(dfsCluster);
    dfsCluster = null;
  }
  
  @Test
  public void testBasic() throws IOException {
    String uri = HdfsTestUtil.getURI(dfsCluster);
    Path lockPath = new Path(uri, "/basedir/lock");
    Configuration conf = HdfsTestUtil.getClientConfiguration(dfsCluster);
    HdfsDirectory dir = new HdfsDirectory(lockPath, conf);
    
    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        Assert.fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }
    // now repeat after close()
    try (Lock lock = dir.obtainLock("testlock")) {
      assert lock != null;
      try (Lock lock2 = dir.obtainLock("testlock")) {
        assert lock2 != null;
        Assert.fail("Locking should fail");
      } catch (LockObtainFailedException lofe) {
        // pass
      }
    }
    dir.close();
  }
}
