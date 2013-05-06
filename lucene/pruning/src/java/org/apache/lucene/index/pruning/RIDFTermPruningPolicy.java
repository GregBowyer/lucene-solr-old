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
import java.util.Collections;
import java.util.Map;

import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.BytesRef;

/**
 * Implementation of {@link TermPruningPolicy} that uses "residual IDF"
 * metric to determine the postings of terms to keep/remove, as defined in
 * <a href="">http://www.dc.fi.udc.es/~barreiro/publications/blanco_barreiro_ecir2007.pdf</a>.
 * <p>Residual IDF measures a difference between a collection-wide IDF of a term
 * (which assumes a uniform distribution of occurrences) and the actual
 * observed total number of occurrences of a term in all documents. Positive
 * values indicate that a term is informative (e.g. for rare terms), negative
 * values indicate that a term is not informative (e.g. too popular to offer
 * good selectivity).
 * <p>This metric produces small values close to [-1, 1], so useful ranges for
 * thresholds under this metrics are somewhere between [0, 1]. The higher the
 * threshold the more informative (and more rare) terms will be retained. For
 * filtering of common words a value of close to or slightly below 0 (e.g. -0.1)
 * should be a good starting point. 
 * 
 */
public class RIDFTermPruningPolicy extends TermPruningPolicy {
  double defThreshold;
  Map<String, Double> thresholds;
  int numDocs;
  private double ridf;

  public RIDFTermPruningPolicy(AtomicReader in,
          Map<String, Integer> fieldFlags, Map<String, Double> thresholds,
          double defThreshold) {
    super(in, fieldFlags);
    this.defThreshold = defThreshold;
    if (thresholds != null) {
      this.thresholds = thresholds;
    } else {
      this.thresholds = Collections.emptyMap();
    }
    this.numDocs = in.numDocs();
  }

  @Override
  public void initPositionsTerm(String field, TermsEnum te) throws IOException {
    double idf = Math.log((double) te.docFreq() / numDocs);

    //double totalFreq = termPositions.freq();
    long totalFreq = te.totalTermFreq();

    double eidf = Math.log(1 - Math.pow(Math.E, -((double) totalFreq / numDocs)));
    this.ridf = -idf + eidf;
  }

  @Override
  public boolean pruneTermsEnum(String field, TermsEnum te) throws IOException {
    return false;
  }

  @Override
  public boolean pruneAllPositions(DocsAndPositionsEnum termPositions, BytesRef t, String field) throws IOException {
    double thr = defThreshold;
    String key = field + ":" + t.toString();
    if (thresholds.containsKey(key)) {
      thr = thresholds.get(key);
    } else if (thresholds.containsKey(field)) {
      thr = thresholds.get(field);
    }

    return ridf <= thr;
  }

  @Override
  public Terms pruneTermVectorTerms(int docNumber, String field, Terms terms) throws IOException {
    return terms;
  }

}
