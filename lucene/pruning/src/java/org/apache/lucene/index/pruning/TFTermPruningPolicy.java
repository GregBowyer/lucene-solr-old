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
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.PruningAtomicReader;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.Terms;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * Policy for producing smaller index out of an input index, by removing postings data
 * for those terms where their in-document frequency is below a specified
 * threshold. 
 * <p>
 * Larger threshold value will produce a smaller index.
 * See {@link TermPruningPolicy} for size vs performance considerations.
 * <p>
 * This implementation uses simple term frequency thresholds to remove all postings
 * from documents where a given term occurs rarely (i.e. its TF in a document
 * is smaller than the threshold).
 * <p>
 * Threshold values in this method are expressed as absolute term frequencies.
 */
public class TFTermPruningPolicy extends TermPruningPolicy {
  protected Map<String,Integer> thresholds;
  protected int defThreshold;
  protected int curThr;

  public TFTermPruningPolicy(AtomicReader in, Map<String,Integer> fieldFlags,
          Map<String,Integer> thresholds, int defThreshold) {
    super(in, fieldFlags);
    this.defThreshold = defThreshold;
    if (thresholds != null) {
      this.thresholds = thresholds;
    } else {
      this.thresholds = Collections.emptyMap();
    }
  }

  @Override
  public boolean pruneTermsEnum(String field, TermsEnum termsEnum) throws IOException {
    // check that at least one doc exceeds threshold
    int thr = defThreshold;

    // TODO [Greg Bowyer] I dont like this, maybe a map of maps ?
    String termKey = field + ":" + termsEnum.term().toString();

    if (thresholds.containsKey(termKey)) {
      thr = thresholds.get(termKey);
    } else if (thresholds.containsKey(field)) {
      thr = thresholds.get(field);
    }
    // END TODO

    Bits liveDocs = in.getLiveDocs();
    DocsEnum td = termsEnum.docs(liveDocs, null, DocsEnum.FLAG_FREQS);

    boolean pass = false;
    do {
      if (td.freq() >= thr) {
        pass = true;
        break;
      }
    } while (td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS);
    return !pass;
  }

  @Override
  public void initPositionsTerm(String field, TermsEnum in) throws IOException {
    // set threshold for this field
    curThr = defThreshold;
    String termKey = field + ":" + in.term().toString();
    if (thresholds.containsKey(termKey)) {
      curThr = thresholds.get(termKey);
    } else if (thresholds.containsKey(field)) {
      curThr = thresholds.get(field);
    }
  }

  @Override
  public boolean pruneAllPositions(DocsAndPositionsEnum termPositions, BytesRef t, String field) throws IOException {
    return termPositions.freq() < curThr;
  }

  @Override
  public Terms pruneTermVectorTerms(final int docNumber, final String field, final Terms terms) throws IOException {
    return new PruningAtomicReader.PruningTerms(this, field, terms);
    /*
    ORIGINAL 3.x code
    int thr = defThreshold;
    if (thresholds.containsKey(field)) {
      thr = thresholds.get(field);
    }
    int removed = 0;
    for (int i = 0; i < terms.length; i++) {
      // check per-term thresholds
      int termThr = thr;
      String t = field + ":" + terms[i];
      if (thresholds.containsKey(t)) {
        termThr = thresholds.get(t);
      }
      if (freqs[i] < termThr) {
        terms[i] = null;
        removed++;
      }      
    }
    return removed;
    */
  }

}
