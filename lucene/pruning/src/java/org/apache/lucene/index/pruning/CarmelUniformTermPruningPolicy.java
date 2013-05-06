package org.apache.lucene.index.pruning;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.similarities.DefaultSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

/**
 * Enhanced implementation of Carmel Uniform Pruning,
 * <p>
 * {@link DocsAndPositionsEnum} whose in-document frequency is below a specified
 * threshold
 * <p>
 * See {@link CarmelTopKTermPruningPolicy} for link to the paper describing this
 * policy. are pruned.
 * <p>
 * Conclusions of that paper indicate that it's best to compute per-term
 * thresholds, as we do in {@link CarmelTopKTermPruningPolicy}. However for
 * large indexes with a large number of terms that method might be too slow, and
 * the (enhanced) uniform approach implemented here may will be faster, although
 * it might produce inferior search quality.
 * <p>
 * This implementation enhances the Carmel uniform pruning approach, as it
 * allows to specify three levels of thresholds:
 * <ul>
 * <li>one default threshold - globally (for terms in all fields)</li>
 * <li>threshold per field</li>
 * <li>threshold per term</li>
 * </ul>
 * <p>
 * These thresholds are applied so that always the most specific one takes
 * precedence: first a per-term threshold is used if present, then per-field
 * threshold if present, and finally the default threshold.
 * <p>
 * Threshold are maintained in a map, keyed by either field names or terms in
 * <code>field:text</code> format. precedence of these values is the following:
 * <p>
 * Thresholds in this method of pruning are expressed as the percentage of the
 * top-N scoring documents per term that are retained. The list of top-N
 * documents is established by using a regular {@link IndexSearcher} and
 * {@link Similarity} to run a simple {@link TermQuery}.
 * <p>
 * Smaller threshold value will produce a smaller index. See
 * {@link TermPruningPolicy} for size vs performance considerations.
 * <p>
 * For indexes with a large number of terms this policy might be still too slow,
 * since it issues a term query for each term in the index. In such situations,
 * the term frequency pruning approach in {@link TFTermPruningPolicy} will be
 * faster, though it might produce inferior search quality.
 */
public class CarmelUniformTermPruningPolicy extends TermPruningPolicy {

  private final float defThreshold;
  private final Map<String,Float> thresholds;
  private final IndexSearcher is;

  private float curThr;
  private int docsPos = 0;
  private ScoreDoc[] docs = null;

  public CarmelUniformTermPruningPolicy(AtomicReader in, Map<String,Integer> fieldFlags,
                                        Map<String,Float> thresholds, float defThreshold,
                                        Similarity sim) {
    super(in, fieldFlags);

    this.defThreshold = defThreshold;
    this.thresholds = thresholds != null ? thresholds : Collections.<String, Float>emptyMap();

    this.is = new IndexSearcher(in);
    is.setSimilarity((sim != null) ? sim : new DefaultSimilarity());
  }
  
  // too costly - pass everything at this stage
  @Override
  public boolean pruneTermsEnum(String field, TermsEnum te) throws IOException {
    return false;
  }
  
  @Override
  public void initPositionsTerm(String field, TermsEnum in) throws IOException {
    curThr = defThreshold;
    String termKey = field + ":" + in.term().toString();
    if (thresholds.containsKey(termKey)) {
      curThr = thresholds.get(termKey);
    } else if (thresholds.containsKey(field)) {
      curThr = thresholds.get(field);
    }
    // calculate count
    int df = in.docFreq();
    int count = Math.round((float) df * curThr);
    if (count < 100) count = 100;
    TopScoreDocCollector collector = TopScoreDocCollector.create(count, true);
    TermQuery tq = new TermQuery(new Term(field, in.term()));
    is.search(tq, collector);
    docs = collector.topDocs().scoreDocs;
    Arrays.sort(docs, ByDocComparator.INSTANCE);
    docsPos = 0;
  }
  
  @Override
  public boolean pruneAllPositions(DocsAndPositionsEnum termPositions, BytesRef t, String field) throws IOException {
    // used up all doc id-s
    if (termPositions.docID() == DocIdSetIterator.NO_MORE_DOCS) {
      return true; // skip any remaining docs
    }

    // TODO [Greg Bowyer] - ReWrite this to use advance()
    while ((docsPos < docs.length - 1) && termPositions.docID() > docs[docsPos].doc) {
      docsPos++;
    }

    if (termPositions.docID() == docs[docsPos].doc) {
      // pass
      docsPos++; // move to next doc id
      return false;
    } else if (termPositions.docID() < docs[docsPos].doc) {
      return true; // skip this one - it's less important
    }
    // should not happen!
    throw new IOException("termPositions.doc > docs[docsPos].doc");
  }
  
  // it probably doesn't make sense to prune term vectors using this method,
  // due to its overhead
  @Override
  public Terms pruneTermVectorTerms(int docNumber, String field, Terms terms) throws IOException {
    return terms;
  }
  
  public static class ByDocComparator implements Comparator<ScoreDoc> {
    public static final ByDocComparator INSTANCE = new ByDocComparator();
    
    public int compare(ScoreDoc o1, ScoreDoc o2) {
      return o1.doc - o2.doc;
    }
  }

}
