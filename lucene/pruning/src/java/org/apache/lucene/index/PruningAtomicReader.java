package org.apache.lucene.index;

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
import java.util.logging.Logger;

import org.apache.lucene.index.pruning.StorePruningPolicy;
import org.apache.lucene.index.pruning.TermPruningPolicy;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

/**
 * This class produces a subset of the input index, by removing some postings
 * data according to rules implemented in a {@link TermPruningPolicy}, and
 * optionally it can also remove stored fields of documents according to rules
 * implemented in a {@link StorePruningPolicy}.
 */
public class PruningAtomicReader extends FilterAtomicReader {
  private static final Logger LOG = Logger.getLogger(PruningAtomicReader.class.getName());
  
  protected int docCount;
  protected int vecCount;
  protected int termCount, delTermCount;
  protected int prunedVecCount, delVecCount;
  
  protected TermPruningPolicy termPolicy;
  protected StorePruningPolicy storePolicy;
  
  /**
   * Constructor.
   * @param in input reader
   * @param storePolicy implementation of {@link StorePruningPolicy} - if null
   *          then stored values will be retained as is.
   * @param termPolicy implementation of {@link TermPruningPolicy}, must not
   * be null.
   */
  public PruningAtomicReader(AtomicReader in, StorePruningPolicy storePolicy,
                             TermPruningPolicy termPolicy) {
    super(in);
    this.termPolicy = termPolicy;
    assert termPolicy != null;
    this.storePolicy = storePolicy;
  }

  @Override
  public Fields fields() throws IOException {
    return new PruningFields(this.termPolicy, super.fields());
  }

  /**
   * Applies a {@link StorePruningPolicy} to stored fields of a document.
   */
  @Override
  public void document(int docID, StoredFieldVisitor visitor) throws IOException {
    docCount++;
    if ((docCount % 10000) == 0) {
      LOG.info(" - stored fields: " + docCount + " docs.");
    }
    if (storePolicy != null) {
      storePolicy.pruneDocument(docID, visitor);
    } else {
      in.document(docID, visitor);
    }
  }

  /**
   * Applies a {@link StorePruningPolicy} to the list of available field infos.
   */
  @Override
  public FieldInfos getFieldInfos() {
    FieldInfos res = super.getFieldInfos();
    if (storePolicy == null) {
      return res;
    }
    return storePolicy.getFieldInfos(res);
  }

  /**
   * Applies {@link TermPruningPolicy} to terms inside term vectors.
   */
  @Override
  public Fields getTermVectors(final int docNumber) throws IOException {
    Fields vectors = super.getTermVectors(docNumber);
    if (vectors == null) {
      return null;
    }

    return new FilterFields(vectors) {
      @Override
      public Terms terms(String field) throws IOException {
        Terms terms = super.terms(field);
        if (terms == null) return null;

        if (termPolicy.pruneWholeTermVector(docNumber, field)) {
          delVecCount++;
          if ((delVecCount % 10000) == 0) {
            LOG.info(" - deleted vectors: " + delVecCount);
          }
          return null;
        }
        return termPolicy.pruneTermVectorTerms(docNumber, field, terms);
      }
    };
  }

  /**
   * Applies {@link TermPruningPolicy} to term positions.
   */
  /*
  @Override
  public TermPositions termPositions() throws IOException {
    return new PruningTermPositions(in.termPositions());
  }
  */
  
  /**
   * Applies {@link TermPruningPolicy} to term enum.
   */
  /*
  @Override
  public TermEnum terms() throws IOException {
    return new PruningTermEnum(in.terms());
  }
  */

  private static final class PruningDocsAndPositionsEnum extends FilterDocsAndPositionsEnum {
    
    protected int[] positions;
    protected DocsAndPositionsEnum tp;

    private final TermPruningPolicy termPolicy;
    private final String field;
    private final BytesRef term;

    //TODO [Greg Bowyer] This is a bit weird
    private final TermsEnum te;

    private PruningDocsAndPositionsEnum(String field, BytesRef term, TermsEnum te,
                                        TermPruningPolicy termPolicy, DocsAndPositionsEnum in) {
      super(in);
      this.tp = in;
      this.field = field;
      this.termPolicy = termPolicy;
      this.term = term;
      this.te = te;
    }

    @Override
    public int nextDoc() throws IOException {
      int nextDoc = super.nextDoc();
      for (;;) {
        positions = null;
        if (nextDoc == NO_MORE_DOCS) {
          return NO_MORE_DOCS;
        }
        termPolicy.initPositionsTerm(this.field, this.te);
        if (!termPolicy.pruneAllPositions(tp, this.term, this.field)) {
          break;
        }
        nextDoc = super.nextDoc();
      }
      return nextDoc;
    }
    
    @Override
    public BytesRef getPayload() throws IOException {
      return termPolicy.prunePayload(field) ? null : super.getPayload();
    }

  }

  public static final class PruningTerms extends FilterTerms {

    private final TermPruningPolicy termPolicy;
    private final String field;

    public PruningTerms(TermPruningPolicy termPolicy, String field, Terms in) {
      super(in);
      this.termPolicy = termPolicy;
      this.field = field;
    }

    @Override
    public TermsEnum iterator(TermsEnum reuse) throws IOException {
      return new PruningAtomicReader.PruningTermsEnum(this.termPolicy, field, in.iterator(reuse));
    }

    //TODO [Greg Bowyer] - Is this correct ? The codec actually stores these things, but we lie
    @Override
    public long size() throws IOException {
      return -1;
    }
  }

  public final static class PruningTermsEnum extends FilterTermsEnum {
    private final String field;
    private final TermPruningPolicy termPolicy;

    // TODO [Greg Bowyer] This is in the wrong place, all over the place
    private long termCount = 0;
    private long delTermCount = 0;

    public PruningTermsEnum(TermPruningPolicy termPolicy, String field, TermsEnum in) {
      super(in);
      this.field = field;
      this.termPolicy = termPolicy;
    }

    @Override
    public boolean seekExact(BytesRef text, boolean useCache) throws IOException {
      boolean toReturn = super.seekExact(text, useCache);
      this.informPolicy();
      return toReturn;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text, boolean useCache) throws IOException {
      SeekStatus toReturn = super.seekCeil(text, useCache);
      this.informPolicy();
      return toReturn;
    }

    @Override
    public void seekExact(long ord) throws IOException {
      super.seekExact(ord);
      this.informPolicy();
    }

    @Override
    public void seekExact(BytesRef term, TermState state) throws IOException {
      super.seekExact(term, state);
      this.informPolicy();
    }

    @Override
    public DocsAndPositionsEnum docsAndPositions(Bits liveDocs, DocsAndPositionsEnum reuse, int flags) throws IOException {
      DocsAndPositionsEnum positionsEnum = super.docsAndPositions(liveDocs, reuse, flags);
      return new PruningDocsAndPositionsEnum(this.field, this.term(), this, this.termPolicy, positionsEnum);
    }

    private void informPolicy() throws IOException {
      termPolicy.initPositionsTerm(this.field, this);
    }

    @Override
    public BytesRef next() throws IOException {
      BytesRef ref;
      while ((ref = super.next()) != null) {
        termCount++;

        if ((termCount % 50000) == 0) {
          LOG.info(" - terms: " + termCount + " (" + term() + "), deleted: " + delTermCount);
        }

        if (termPolicy.pruneAllFieldPostings(field) || termPolicy.pruneTermsEnum(field, in)) {
          delTermCount++;
          // System.out.println("TE: remove " + term());
          continue;
        } else {
          break;
        }
      }
      return ref;
    }
  }

  private class PruningFields extends FilterFields {
    private final TermPruningPolicy termPolicy;

    public PruningFields(TermPruningPolicy termPolicy, Fields fields) {
      super(fields);
      this.termPolicy = termPolicy;
    }

    @Override
    public Terms terms(String field) throws IOException {
      return new PruningTerms(termPolicy, field, super.terms(field));
    }
  }
}