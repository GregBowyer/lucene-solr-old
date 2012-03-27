/**
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
package org.apache.solr.hadoop;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SolrOutputFormat<K, V> extends FileOutputFormat<K, V> {

  private static final Log LOG = LogFactory.getLog(SolrOutputFormat.class);

  /**
   * The key used to pass the boolean configuration parameter that instructs for
   * regular or zip file output
   */
  public static final String OUTPUT_ZIP_FILE = "solr.output.zip.format";

  static int defaultSolrWriterThreadCount = 2;

  public static final String SOLR_WRITER_THREAD_COUNT = "solr.record.writer.num.threads";

  static int defaultSolrWriterQueueSize = 100;

  public static final String SOLR_WRITER_QUEUE_SIZE = "solr.record.writer.max.queues.size";

  static int defaultSolrBatchSize = 20;

  public static final String SOLR_RECORD_WRITER_BATCH_SIZE = "solr.record.writer.batch.size";

  /** Get the number of threads used for index writing */
  public static void setSolrWriterThreadCount(int count, Configuration conf) {
    conf.setInt(SOLR_WRITER_THREAD_COUNT, count);
  }

  /** Set the number of threads used for index writing */
  public static int getSolrWriterThreadCount(Configuration conf) {
    return conf.getInt(SOLR_WRITER_THREAD_COUNT, defaultSolrWriterThreadCount);
  }

  /**
   * Set the maximum size of the the queue for documents to be written to the
   * index.
   */
  public static void setSolrWriterQueueSize(int count, Configuration conf) {
    conf.setInt(SOLR_WRITER_QUEUE_SIZE, count);
  }

  /** Return the maximum size for the number of documents pending index writing. */
  public static int getSolrWriterQueueSize(Configuration conf) {
    return conf.getInt(SOLR_WRITER_QUEUE_SIZE, defaultSolrWriterQueueSize);
  }

  /**
   * configure the job to output zip files of the output index, or full
   * directory trees. Zip files are about 1/5th the size of the raw index, and
   * much faster to write, but take more cpu to create. zip files are ideal for
   * deploying into a katta managed shard.
   * 
   * @param output
   * @param conf
   */
  public static void setOutputZipFormat(boolean output, Configuration conf) {
    conf.setBoolean(OUTPUT_ZIP_FILE, output);
  }

  /**
   * return true if the output should be a zip file of the index, rather than
   * the raw index
   * 
   * @param conf
   * @return
   */
  public static boolean isOutputZipFormat(Configuration conf) {
    return conf.getBoolean(OUTPUT_ZIP_FILE, false);
  }

  @Override
  public void checkOutputSpecs(JobContext job) throws IOException {
    super.checkOutputSpecs(job);
    if (job.getConfiguration().get(SolrHadoopUtils.SETUP_OK) == null) {
      throw new IOException("Solr home cache not set up!");
    }
  }


  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    return new SolrRecordWriter<K, V>(context);
  }

  public static int getBatchSize(Configuration jobConf) {
    // TODO Auto-generated method stub
    return jobConf.getInt(SolrOutputFormat.SOLR_RECORD_WRITER_BATCH_SIZE,
        defaultSolrBatchSize);
  }

  public static void setBatchSize(int count, Configuration jobConf) {
    jobConf.setInt(SOLR_RECORD_WRITER_BATCH_SIZE, count);
  }

}
