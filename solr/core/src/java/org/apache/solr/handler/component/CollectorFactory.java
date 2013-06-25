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

package org.apache.solr.handler.component;

import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.solr.search.*;
import org.apache.solr.search.SolrIndexSearcher.QueryCommand;
import org.apache.solr.common.params.ShardParams;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.schema.SchemaField;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.HashMap;

public class CollectorFactory
{
  private String name;
  private int ordinal = Integer.MAX_VALUE;

  public CollectorFactory(){
    super();
  }

  public void setName(String name) {
      this.name = name;
  }

  public String getName() {
      return this.name;
  }

  public int getOrdinal() {
      return this.ordinal;
  }

  public void setOrdinal(int ordinal) {
      this.ordinal = ordinal;
  }

  public String getId() {
      return "cl.analytic."+this.ordinal;
  }

  public Collector getCollector(SolrIndexSearcher indexSearcher, CollectorSpec spec, QueryCommand cmd, int len, boolean needScores) throws IOException{
    TopDocsCollector topCollector;
      
    if(cmd.getSort() == null) {
      System.out.println("Ranking collector");

      if(cmd.getScoreDoc() != null) {
        topCollector = TopScoreDocCollector.create(len, cmd.getScoreDoc(), true); //create the Collector with InOrderPagingCollector
      } else {
        topCollector = TopScoreDocCollector.create(len, true);
      }
    } else {
      System.out.println("Sorting collector");
      topCollector = TopFieldCollector.create(indexSearcher.weightSort(cmd.getSort()), len, false, needScores, needScores, true);
    }

    return topCollector;
  }
	
  public DocSetDelegateCollector getDocSetCollector(SolrIndexSearcher indexSearcher, CollectorSpec spec, QueryCommand cmd, int len, int maxDoc, boolean needScores) throws IOException{
    TopDocsCollector topCollector = (TopDocsCollector)getCollector(indexSearcher, spec, cmd, len, needScores);
    return new DocSetDelegateCollector(maxDoc>>6, maxDoc, topCollector);
  }

  public void merge(ResponseBuilder rb, ShardRequest sreq) {
    SortSpec ss = rb.getSortSpec();
    Sort sort = ss.getSort();

    SortField[] sortFields = null;
    if(sort != null) sortFields = sort.getSort();
    else {
        sortFields = new SortField[]{SortField.FIELD_SCORE};
    }
 
    SchemaField uniqueKeyField = rb.req.getSchema().getUniqueKeyField();

    // id to shard mapping, to eliminate any accidental dups
    HashMap<Object,String> uniqueDoc = new HashMap<Object,String>();

    // Merge the docs via a priority queue so we don't have to sort *all* of the
    // documents... we only need to order the top (rows+start)
    ShardFieldSortedHitQueue queue;
    queue = new ShardFieldSortedHitQueue(sortFields, ss.getOffset() + ss.getCount());

    NamedList<Object> shardInfo = null;
    if(rb.req.getParams().getBool(ShardParams.SHARDS_INFO, false)) {
      shardInfo = new SimpleOrderedMap<Object>();
      rb.rsp.getValues().add(ShardParams.SHARDS_INFO,shardInfo);
    }
      
    long numFound = 0;
    Float maxScore=null;
    boolean partialResults = false;
    for (ShardResponse srsp : sreq.responses) {
      SolrDocumentList docs = null;

      if(shardInfo!=null) {
        SimpleOrderedMap<Object> nl = new SimpleOrderedMap<Object>();
          
        if (srsp.getException() != null) {
          Throwable t = srsp.getException();
          if(t instanceof SolrServerException) {
            t = ((SolrServerException)t).getCause();
          }
          nl.add("error", t.toString() );
          StringWriter trace = new StringWriter();
          t.printStackTrace(new PrintWriter(trace));
          nl.add("trace", trace.toString() );
        } else {
          docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
          nl.add("numFound", docs.getNumFound());
          nl.add("maxScore", docs.getMaxScore());
        }
        if(srsp.getSolrResponse()!=null) {
          nl.add("time", srsp.getSolrResponse().getElapsedTime());
        }

        shardInfo.add(srsp.getShard(), nl);
      }

      // now that we've added the shard info, let's only proceed if we have no error.
      if (srsp.getException() != null) {
        continue;
      }

      if (docs == null) { // could have been initialized in the shards info block above
        docs = (SolrDocumentList)srsp.getSolrResponse().getResponse().get("response");
      }
        
      NamedList<?> responseHeader = (NamedList<?>)srsp.getSolrResponse().getResponse().get("responseHeader");
      if (responseHeader != null && Boolean.TRUE.equals(responseHeader.get("partialResults"))) {
        partialResults = true;
      }
        
      // calculate global maxScore and numDocsFound
      if (docs.getMaxScore() != null) {
        maxScore = maxScore==null ? docs.getMaxScore() : Math.max(maxScore, docs.getMaxScore());
      }

      numFound += docs.getNumFound();

      NamedList sortFieldValues = (NamedList)(srsp.getSolrResponse().getResponse().get("sort_values"));

      // go through every doc in this response, construct a ShardDoc, and
      // put it in the priority queue so it can be ordered.
      for (int i=0; i<docs.size(); i++) {
        SolrDocument doc = docs.get(i);
        Object id = doc.getFieldValue(uniqueKeyField.getName());

        String prevShard = uniqueDoc.put(id, srsp.getShard());
        if (prevShard != null) {
          // duplicate detected
          numFound--;

          // For now, just always use the first encountered since we can't currently
          // remove the previous one added to the priority queue.  If we switched
          // to the Java5 PriorityQueue, this would be easier.
          continue;
          // make which duplicate is used deterministic based on shard
          // if (prevShard.compareTo(srsp.shard) >= 0) {
          //  TODO: remove previous from priority queue
          //  continue;
        }

        ShardDoc shardDoc = new ShardDoc();
        shardDoc.id = id;
        shardDoc.shard = srsp.getShard();
        shardDoc.orderInShard = i;
        Object scoreObj = doc.getFieldValue("score");
        if (scoreObj != null) {
          if (scoreObj instanceof String) {
            shardDoc.score = Float.parseFloat((String)scoreObj);
          } else {
            shardDoc.score = (Float)scoreObj;
          }
        }

        shardDoc.sortFieldValues = sortFieldValues;

        queue.insertWithOverflow(shardDoc);
      } // end for-each-doc-in-response
    } // end for-each-response

    // The queue now has 0 -> queuesize docs, where queuesize <= start + rows
    // So we want to pop the last documents off the queue to get
    // the docs offset -> queuesize
    int resultSize = queue.size() - ss.getOffset();
    resultSize = Math.max(0, resultSize);  // there may not be any docs in range

    Map<Object,ShardDoc> resultIds = new HashMap<Object,ShardDoc>();
    for (int i=resultSize-1; i>=0; i--) {
      ShardDoc shardDoc = queue.pop();
      shardDoc.positionInResponse = i;
      // Need the toString() for correlation with other lists that must
      // be strings (like keys in highlighting, explain, etc)
      resultIds.put(shardDoc.id.toString(), shardDoc);
    }

    // Add hits for distributed requests
    // https://issues.apache.org/jira/browse/SOLR-3518
    rb.rsp.addToLog("hits", numFound);
    SolrDocumentList responseDocs = new SolrDocumentList();
    if (maxScore!=null) responseDocs.setMaxScore(maxScore);
    responseDocs.setNumFound(numFound);
    responseDocs.setStart(ss.getOffset());
    // size appropriately
    for (int i=0; i<resultSize; i++) responseDocs.add(null);

    // save these results in a private area so we can access them
    // again when retrieving stored fields.
    // TODO: use ResponseBuilder (w/ comments) or the request context?
    rb.resultIds = resultIds;
    rb._responseDocs = responseDocs;
    if (partialResults) {
      rb.rsp.getResponseHeader().add( "partialResults", Boolean.TRUE );
    }
  }
}
