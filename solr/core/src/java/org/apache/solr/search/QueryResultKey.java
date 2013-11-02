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

package org.apache.solr.search;

import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import java.util.List;

/** A hash key encapsulating a query, a list of filters, and a sort
 *
 */
public final class QueryResultKey {
  final Query query;
  final Sort sort;
  final Filter filter;
  final SortField[] sfields;
  final int nc_flags;  // non-comparable flags... ignored by hashCode and equals

  private final int hc;  // cached hashCode

  private static SortField[] defaultSort = new SortField[0];

  public QueryResultKey(Query query, Filter filter, Sort sort, int nc_flags) {
    this.query = query;
    this.sort = sort;
    this.filter = filter;
    this.nc_flags = nc_flags;

    int h = query.hashCode();

    sfields = (this.sort !=null) ? this.sort.getSort() : defaultSort;
    for (SortField sf : sfields) {
      h = h*29 + sf.hashCode();
    }

    if (filter != null) {
      h ^= filter.hashCode();
    }

    hc = h;
  }

  @Override
  public int hashCode() {
    return hc;
  }

  @Override
  public boolean equals(Object o) {
    if (o==this) return true;
    if (!(o instanceof QueryResultKey)) return false;
    QueryResultKey other = (QueryResultKey)o;

    // check for the thing most likely to be different (and the fastest things)
    // first.
    if (this.sfields.length != other.sfields.length) return false;
    if (!this.query.equals(other.query)) return false;

    for (int i=0; i<sfields.length; i++) {
      SortField sf1 = this.sfields[i];
      SortField sf2 = other.sfields[i];
      if (!sf1.equals(sf2)) return false;
    }

    return
        (this.filter == null && other.filter == null) ||
        (this.filter != null && this.filter.equals(other.filter));
  }

}
