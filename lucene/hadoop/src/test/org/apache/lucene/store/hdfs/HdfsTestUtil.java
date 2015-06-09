package org.apache.lucene.store.hdfs;

import java.io.File;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.hdfs.server.namenode.ha.HATestUtil;
import org.apache.lucene.util.LuceneTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class HdfsTestUtil {
  private static Logger log = LoggerFactory.getLogger(HdfsTestUtil.class);
  
  private static final String LOGICAL_HOSTNAME = "ha-nn-uri-%d";

  private static Locale savedLocale;
  private static Map<MiniDFSCluster,Timer> timers = new ConcurrentHashMap<>();

  public static MiniDFSCluster setupClass(String dir) throws Exception {
    LuceneTestCase.assumeFalse("HDFS tests were disabled by -Dtests.disableHdfs",
      Boolean.parseBoolean(System.getProperty("tests.disableHdfs", "false")));

    savedLocale = Locale.getDefault();
    // TODO: we HACK around HADOOP-9643
    Locale.setDefault(Locale.ENGLISH);

    Configuration conf = new Configuration();
    conf.set("dfs.block.access.token.enable", "false");
    conf.set("dfs.permissions.enabled", "false");
    conf.set("hadoop.security.authorization", "false");
    conf.set("hadoop.security.authentication", "simple");
    conf.set("hdfs.minidfs.basedir", dir + File.separator + "hdfsBaseDir");
    conf.set("dfs.namenode.name.dir", dir + File.separator + "nameNodeNameDir");
    conf.setBoolean("fs.hdfs.impl.disable.cache", true);

    System.setProperty("test.build.data", dir + File.separator + "hdfs" + File.separator + "build");
    System.setProperty("test.cache.data", dir + File.separator + "hdfs" + File.separator + "cache");
    System.setProperty("java.security.debug", "all");
    System.setProperty("javax.security.debug", "all");

    final MiniDFSCluster dfsCluster = new MiniDFSCluster.Builder(conf)
        .format(true)
        .numDataNodes(2)
        .build();
    dfsCluster.waitActive();

    int rndMode = LuceneTestCase.random().nextInt(3);
    if (true && rndMode == 1) {
      NameNodeAdapter.enterSafeMode(dfsCluster.getNameNode(), false);

      int rnd = LuceneTestCase.random().nextInt(10000);
      Timer timer = new Timer();
      timers.put(dfsCluster, timer);
      timer.schedule(new TimerTask() {

        @Override
        public void run() {
          NameNodeAdapter.leaveSafeMode(dfsCluster.getNameNode());
        }
      }, rnd);
    }

    return dfsCluster;
  }

  public static Configuration getClientConfiguration(MiniDFSCluster dfsCluster) {
    if (dfsCluster.getNameNodeInfos().length > 1) {
      Configuration conf = new Configuration();
      HATestUtil.setFailoverConfigurations(dfsCluster, conf);
      return conf;
    } else {
      return new Configuration();
    }
  }
  
  public static void teardownClass(MiniDFSCluster dfsCluster) throws Exception {
    System.clearProperty("test.build.data");
    System.clearProperty("test.cache.data");
    if (dfsCluster != null) {
      Timer timer = timers.remove(dfsCluster);
      if (timer != null) {
        timer.cancel();
      }
      try {
        dfsCluster.shutdown();
      } catch (Error e) {
        // Added in SOLR-7134
        // Rarely, this can fail to either a NullPointerException
        // or a class not found exception. The later may fixable
        // by adding test dependencies.
        log.warn("Exception shutting down dfsCluster", e);
      }
    }
    
    // TODO: we HACK around HADOOP-9643
    if (savedLocale != null) {
      Locale.setDefault(savedLocale);
    }
  }
  
  public static String getURI(MiniDFSCluster dfsCluster) {
    if (dfsCluster.getNameNodeInfos().length > 1) {
      String logicalName = String.format(Locale.ENGLISH, LOGICAL_HOSTNAME, dfsCluster.getInstanceId()); // NOTE: hdfs uses default locale
      return "hdfs://" + logicalName;
    } else {
      URI uri = dfsCluster.getURI(0);
      return uri.toString() ;
    }
  }

}
