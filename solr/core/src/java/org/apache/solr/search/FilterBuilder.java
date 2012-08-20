package org.apache.solr.search;

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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.lucene.queries.BooleanFilter;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.FilterCache;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.solr.request.SolrQueryRequest;

import java.util.Arrays;
import java.util.List;

/**
 * Very naive implementation of code that hopefully will eventually be positioned
 * to be able to create sensible lucene filters in a fashion that are usable by
 * solr
 *
 * For now this essentially creates one simple BooleanFilter that does nothing more
 * than wrap a lot of query filters
 *
 * In good time the aim is to get this to do the following:
 *
 *  ∘ Handle setting up filter implementations that are able to correctly cache
 *    with reference to the AtomicReader that they are caching for rather that for
 *    the entire index at large
 *
 *  ∘ Get the post filters working, I am thinking that this can be done via lucenes
 *    chained filter, with the ‟expensive” filters being put towards the end of the
 *    chain - this has different semantics internally to the original implementation
 *    but IMHO should have the same result for end users
 *
 *  ∘ Learn how to create filters that are potentially more efficient, at present solr
 *    basically runs a simple query that gathers a DocSet that relates to the documents
 *    that we want filtered; it would be interesting to make use of filter implementations
 *    that are in theory faster than query filters (for instance there are filters that are
 *    able to query the FieldCache)
 *
 *  ∘ Learn how to decompose filters so that a complex filter query can be cached (potentially)
 *    as its constituent parts; for example the filter below currently needs love, care and feeding
 *    to ensure that the filter cache is not unduly stressed
 *
 *    'category:(100) OR category:(200) OR category:(300)'
 *    Really there is no reason not to express this in a cached form as
 *    BooleanFilter(
 *      FilterClause(CachedFilter(TermFilter(Term("category", 100))), SHOULD),
 *      FilterClause(CachedFilter(TermFilter(Term("category", 200))), SHOULD),
 *      FilterClause(CachedFilter(TermFilter(Term("category", 300))), SHOULD)
 *    )
 *
 *    This would yield better cache usage I think as we can reuse docsets across multiple queries
 *    as well as avoid issues when filters are presented in differing orders
 *
 *  ∘ Instead of end users providing costing we might (and this is a big might FWIW), be able to create
 *    a sort of execution plan of filters, leveraging a combination of what the index is able to tell us
 *    as well as sampling and ‟educated guesswork”; in essence this is what some DBMS software, like
 *    postgresql does - it has a genetic algo that attempts to solve the travelling salesman - to great
 *    effect
 *
 *   ∘ I am sure I will probably come up with other ambitious ideas to plug in here ..... :S 
 */
public class FilterBuilder {
  
  private final List<String> rawFqs;
  private final Iterable<Query> queryFqs;

  // TODO - I want this to go away its is not nice on the law of demeter front
  private final SolrQueryRequest req;
  private final FilterCache filterCache;

  private FilterBuilder(String[] fqs, SolrQueryRequest req) {
    this.rawFqs = fqs == null ? ImmutableList.<String>of() : Arrays.asList(fqs);
    this.req = req;
    this.queryFqs = ImmutableList.of();
    this.filterCache = initFilterCache(req);
  }

  public FilterBuilder(Iterable<Query> filterQueries, SolrQueryRequest req) {
    this.queryFqs = filterQueries;
    this.req = req;
    this.rawFqs = ImmutableList.of();
    this.filterCache = initFilterCache(req);
  }

  private static FilterCache initFilterCache(SolrQueryRequest req) {
    SolrCache cache = req.getSearcher().getFilterCache();
    return (cache != null) ? new SolrCacheMapAdapter(cache) : null;
  }

  public static FilterBuilder getFilterBuilder(SolrQueryRequest req, String... fqs) {
    return new FilterBuilder(fqs, req);
  }

  public static FilterBuilder getFilterBuilder(SolrQueryRequest req, Iterable<Query> filterQueries) {
    return new FilterBuilder(filterQueries, req);
  }

  // TODO [Greg] Remove ? the Map adapter ? Make a FilterCache interface inside Lucene core ?  ?
  private Filter wrapWithCacheIfNeeded(Filter filter, boolean cacheFilter) {
    if (this.filterCache != null && cacheFilter) {
      return new CachingWrapperFilter(filter, filterCache);
    }

    // No caching should be done, either the end user disabled caches, or this specific filter
    // is not suitable for caching
    return filter;
  }

  // TODO - Probably should throw a better exception than ParseException ?
  public Filter getFilter() throws SyntaxError {
    if (this.rawFqs.isEmpty() && Iterables.isEmpty(this.queryFqs))
      return null;

    BooleanFilter topFilter = new BooleanFilter();
    BooleanFilter shoulds = new BooleanFilter();

    //TODO Longterm it would be nice if QueryUtils.isNegative was able to handle more than
    // boolean query
    for (String rawFQ : rawFqs) {
      Query q = QParser.getParser(rawFQ.trim(), null, this.req).getQuery();
      if (q != null) {
        boolean cache = true;
        int cost = 0;

        if (q instanceof ExtendedQuery) {
          cache = ((ExtendedQuery) q).getCache();
          cost = ((ExtendedQuery) q).getCost();
        }

        Query abs = QueryUtils.getAbs(q);
        boolean positive = abs == q;
        if (positive) {
          shoulds.add(wrapWithCacheIfNeeded(new QueryWrapperFilter(q), cache), BooleanClause.Occur.MUST);
        } else {
          topFilter.add(wrapWithCacheIfNeeded(new QueryWrapperFilter(abs), cache), BooleanClause.Occur.MUST_NOT);
        }
      }
    }

    // TODO [Greg] this is fugly
    // HACK  HACK  HACK  HACK  HACK  HACK  HACK  HACK  HACK  HACK
    //-----------------------------------------------------------
    for (Query q : queryFqs) {
      if (q != null) {
        Query abs = QueryUtils.getAbs(q);
        boolean positive = abs == q;
        if (positive) {
          shoulds.add(new QueryWrapperFilter(q), BooleanClause.Occur.MUST);
        } else {
          topFilter.add(new QueryWrapperFilter(abs), BooleanClause.Occur.MUST_NOT);
        }
      }
    }
    //-----------------------------------------------------------
    // HACK  HACK  HACK  HACK  HACK  HACK  HACK  HACK  HACK  HACK

    if (!shoulds.clauses().isEmpty()) {
      topFilter.add(shoulds, BooleanClause.Occur.SHOULD);
      return topFilter;
    } else if (shoulds.clauses().isEmpty() && topFilter.clauses().isEmpty()) {
      return null;
    } else if (shoulds.clauses().isEmpty() && topFilter.clauses().isEmpty()) {
      return shoulds;
    } else {
      return topFilter;
    }
  }

  private static final class SolrCacheMapAdapter implements FilterCache {
    private final SolrCache cache;

    public SolrCacheMapAdapter(SolrCache underlyingCache) {
      this.cache = underlyingCache;
    }

    @Override
    public DocIdSet get(Object readerKey, Filter filterKey) {
      return (DocIdSet) this.cache.get(new SolrCacheAdapterKey(readerKey, filterKey));
    }

    @Override
    public void put(Object readerKey, Filter filterKey, DocIdSet toCache) {
      this.cache.put(new SolrCacheAdapterKey(readerKey, filterKey), toCache);
    }

    @Override
    public long sizeInBytes() {
      // TODO [Greg] This is inaccurate
      return RamUsageEstimator.sizeOf(this.cache);
    }

    private static final class SolrCacheAdapterKey {
      private final Object readerKey;
      private final Filter filter;

      public SolrCacheAdapterKey(Object readerKey, Filter filter) {
        this.readerKey = readerKey;
        this.filter = filter;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SolrCacheAdapterKey that = (SolrCacheAdapterKey) o;
        return filter.equals(that.filter) && readerKey.equals(that.readerKey);
      }

      @Override
      public int hashCode() {
        int result = readerKey.hashCode();
        result = 31 * result + filter.hashCode();
        return result;
      }
    }
  }
}
