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

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import org.apache.solr.search.*;
import org.apache.solr.search.SolrIndexSearcher.QueryCommand;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;

public class SumCollectorFactory extends CollectorFactory {

    public SumCollectorFactory(){
        super();
    }

    public Collector getCollector(SolrIndexSearcher indexSearcher, CollectorSpec spec, QueryCommand cmd, int len, boolean needScores) throws IOException{
        return new SumCollector(getId(), spec, cmd);
    }

    public void merge(ResponseBuilder rb, ShardRequest sreq) {

        Map<String, Float> merged = new HashMap<String, Float>();
        String id = getId();
        for (ShardResponse srsp : sreq.responses) {

          Map<String, Float> data = (Map<String, Float>) srsp.getSolrResponse().getResponse().get(id);

          for (Map.Entry<String, Float> entry : data.entrySet()) {
            String name = entry.getKey();
            float v = entry.getValue();

            if (merged.containsKey(name)) {
              Float f = merged.get(name);
              merged.put(name, f + v);
            } else {
              merged.put(name, v);
            }
          }
        }

        rb.getResponse().add(id, merged);
    }

    class SumCollector extends DelegatingCollector {

        private String groupby;
        private String sum;
        private Float count = new Float("0.0");
        private Map<String, Float> groupbyTable;
        private FieldCache.Floats sumValues;
        private BinaryDocValues groupbyValues;

        public SumCollector(String id, CollectorSpec spec, QueryCommand cmd) {
            groupby = spec.getParams().get("groupby");
            sum = spec.getParams().get("column");
            groupbyTable = new HashMap<String, Float>();
            if(groupby == null) {
                groupbyTable.put("sum", count);
            }

            cmd.getResponseBuilder().getResponse().add(id, groupbyTable);
        }

        public void setNextReader(AtomicReaderContext context) throws IOException {
            sumValues = FieldCache.DEFAULT.getFloats(context.reader(),sum, false);
            if(groupby != null) {
                groupbyValues = FieldCache.DEFAULT.getTerms(context.reader(), groupby);
            }

            getDelegate().setNextReader(context);
        }

        private final BytesRef term = new BytesRef();

        public void collect(int doc) throws IOException {
            if(groupby != null) {
                groupbyValues.get(doc, term);
                String groupbyValue = term.utf8ToString();
                Float value = groupbyTable.get(groupbyValue);

                if(value == null) {
                   groupbyTable.put(groupbyValue, sumValues.get(doc));
                } else {
                   groupbyTable.put(groupbyValue, value + sumValues.get(doc));
                }
            } else {
                Float value = groupbyTable.get("sum");
                groupbyTable.put("sum", value + sumValues.get(doc));
            }

            getDelegate().collect(doc);
        }

        public boolean acceptsDocsOutOfOrder() {
            return getDelegate().acceptsDocsOutOfOrder();
        }

        public void setScorer(Scorer scorer) throws IOException {
            getDelegate().setScorer(scorer);
        }
    }
}
