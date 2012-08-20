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

package org.apache.solr.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.queries.FilterClause;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.search.QueryResultKey;
import org.junit.Test;

public class QueryResultKeyTest extends SolrTestCaseJ4 {

  @Test
  public void testFiltersHashCode() {
    // the hashcode should be the same even when the list
    // of filters is in a different order
    
    Sort sort = new Sort(new SortField("test", SortField.Type.INT));
    BooleanFilter filter = new BooleanFilter();
    filter.add(new TermsFilter(new Term("test", "field")), Occur.MUST);
    filter.add(new TermsFilter(new Term("test2", "field2")), Occur.MUST);

    BooleanQuery query = new BooleanQuery();
    query.add(new TermQuery(new Term("test", "field")), Occur.MUST);
    
    QueryResultKey qrk1 = new QueryResultKey(query , filter, sort, 1);

    BooleanQuery query2 = new BooleanQuery();
    query2.add(new TermQuery(new Term("test", "field")), Occur.MUST);

    BooleanFilter filter2 = new BooleanFilter();
    filter2.add(new TermsFilter(new Term("test", "field")), Occur.MUST);
    filter2.add(new TermsFilter(new Term("test2", "field2")), Occur.MUST);

    assertEquals(filter.hashCode(), filter2.hashCode());

    QueryResultKey qrk2 = new QueryResultKey(query2, filter2, sort, 1);
    
    assertEquals(qrk1.hashCode(), qrk2.hashCode());
    assertEquals(qrk1, qrk2);
  }

  @Test
  public void testQueryResultKeySortedFilters() {
    Query fq1 = new TermQuery(new Term("test1", "field1"));

    Query query = new TermQuery(new Term("test3", "field3"));

    BooleanFilter filter = new BooleanFilter();
    filter.add(new QueryWrapperFilter(fq1), Occur.MUST);
    filter.add(new TermFilter(new Term("test2", "field2")), Occur.MUST);

    QueryResultKey key = new QueryResultKey(query, filter, null, 0);

    BooleanFilter filter2 = new BooleanFilter();
    filter.add(new TermFilter(new Term("test2", "field2")), Occur.MUST);
    filter.add(new QueryWrapperFilter(fq1), Occur.MUST);

    QueryResultKey newKey = new QueryResultKey(query, filter2, null, 0);

    assertEquals(key, newKey);
  }

}
