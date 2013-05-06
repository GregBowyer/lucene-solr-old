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
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.pruning.CarmelTopKTermPruningPolicy;
import org.apache.lucene.index.pruning.PruningPolicy;
import org.apache.lucene.index.pruning.RIDFTermPruningPolicy;
import org.apache.lucene.index.pruning.StorePruningPolicy;
import org.apache.lucene.index.pruning.TFTermPruningPolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.LuceneTestCase;


public class TestPruningReader extends LuceneTestCase {

  // parameters for the Carmel-TopK-Pruning 
  private static final int R = 1; //number of terms in the query
  private static final int K = 2; // top K results
  private static final float EPSILON = .001f; // error in score

  RAMDirectory sourceDir = new RAMDirectory();

  /** once computed base on how index is created, these are the full scores, i.e. before pruning */ 
  private static Map<Term,ScoreDoc[]> fullScores = initFullScores(); 
  private static Map<Term,ScoreDoc[]> prunedScores = initPrunedScores(); 
  
  private void assertTD(AtomicReader ir, Term t, int[] ids) throws Exception {
    DocsAndPositionsEnum td = ir.termPositionsEnum(t);
    assertNotNull(td);
    int i = 0;
    while(td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
      int doc = td.docID();
      assertEquals(t + ", i=" + i, ids[i], doc);
      i++;
    }
    assertEquals(ids.length, i);
  }
  
  /**
   * Scores of the full, unpruned index.
   */
  private static Map<Term, ScoreDoc[]> initFullScores() {
    HashMap<Term, ScoreDoc[]> res = new HashMap<Term, ScoreDoc[]>();
    Term t;
    ScoreDoc sd[]; 
    t = new Term("body","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 0.7154686450958252f),
        new ScoreDoc(2, 0.5310977101325989f),
        new ScoreDoc(3, 0.5310977101325989f),
        new ScoreDoc(1, 0.4336394667625427f),
        new ScoreDoc(0, 0.40883922576904297f)
        };
    res.put(t,sd);
    t = new Term("body","two");
    sd = new ScoreDoc[] {
        new ScoreDoc(2, 0.6495190262794495f),
        new ScoreDoc(1, 0.5303300619125366f),
        new ScoreDoc(0, 0.5f),
        new ScoreDoc(4, 0.4375f)
    };
    res.put(t,sd);
    t = new Term("body","three");
    sd = new ScoreDoc[] {
        new ScoreDoc(3, 0.7944550514221191f),
        new ScoreDoc(1, 0.6486698389053345f),
        new ScoreDoc(0, 0.6115717887878418f)
    };
    res.put(t,sd);
    t = new Term("test","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 0.8468888998031616f)
    };
    res.put(t,sd);
    t = new Term("allthesame","allthesame"); 
    sd = new ScoreDoc[] {
        new ScoreDoc(0, 0.8176784515380859f),
        new ScoreDoc(1, 0.8176784515380859f),
        new ScoreDoc(2, 0.8176784515380859f),
        new ScoreDoc(3, 0.8176784515380859f),
        new ScoreDoc(4, 0.8176784515380859f)
    };
    res.put(t,sd);
    return res;
  }

  /**
   * Expected scores of the pruned index - with EPSILON=0.001, K=2, R=1 
   */
  private static Map<Term, ScoreDoc[]> initPrunedScores() {
    HashMap<Term, ScoreDoc[]> res = new HashMap<Term, ScoreDoc[]>();
    Term t;
    ScoreDoc sd[]; 
    t = new Term("body","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 0.74011815f),
        new ScoreDoc(2, 0.54939526f),
        new ScoreDoc(3, 0.54939526f),
    };
    res.put(t,sd);
    t = new Term("body","two");
    sd = new ScoreDoc[] {
        new ScoreDoc(2, 0.7679404f),
        new ScoreDoc(1, 0.62702066f),
    };
    res.put(t,sd);
    t = new Term("body","three");
    sd = new ScoreDoc[] {
        new ScoreDoc(3, 0.7679404f),
        new ScoreDoc(1, 0.62702066f),
    };
    res.put(t,sd);
    t = new Term("test","one");
    sd = new ScoreDoc[] {
        new ScoreDoc(4, 2.9678855f)
    };
    res.put(t,sd);
    t = new Term("allthesame","allthesame"); // must keep all because all are the same! 
    sd = new ScoreDoc[] {
        new ScoreDoc(0, 0.84584934f),
        new ScoreDoc(1, 0.84584934f),
        new ScoreDoc(2, 0.84584934f),
        new ScoreDoc(3, 0.84584934f),
        new ScoreDoc(4, 0.84584934f)
    };
    res.put(t,sd);
    return res;
  }

  private void assertTDCount(AtomicReader ir, Term t, int count) throws Exception {
    DocsAndPositionsEnum td = ir.termPositionsEnum(t);
    assertNotNull(td);
    int i = 0;
    while (td.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) i++;
    assertEquals(t.toString(), count, i);
  }
  
  public void setUp() throws Exception {
    super.setUp();

    FieldType storedNotIndexed = new FieldType(TextField.TYPE_STORED);
    storedNotIndexed.setIndexed(false);

    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(sourceDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    Document doc = new Document();
    doc.add(new Field("body", "one two three four", TextField.TYPE_STORED));
    doc.add(new Field("id", "0", storedNotIndexed));
    doc.add(new Field("allthesame", "allthesame", TextField.TYPE_STORED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one two three one two three", TextField.TYPE_STORED));
    doc.add(new Field("id", "1", storedNotIndexed));
    doc.add(new Field("allthesame", "allthesame", TextField.TYPE_STORED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one two one two one two", TextField.TYPE_STORED));
    doc.add(new Field("id", "2", storedNotIndexed));
    doc.add(new Field("allthesame", "allthesame", TextField.TYPE_STORED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one three one three one three", TextField.TYPE_STORED));
    doc.add(new Field("id", "3", storedNotIndexed));
    doc.add(new Field("allthesame", "allthesame", TextField.TYPE_STORED));
    iw.addDocument(doc);
    doc = new Document();
    doc.add(new Field("body", "one one one one two", TextField.TYPE_STORED));
    doc.add(new Field("test", "one two one two three three three four", TextField.TYPE_STORED));
    doc.add(new Field("id", "4", storedNotIndexed));
    doc.add(new Field("allthesame", "allthesame", TextField.TYPE_STORED));
    iw.addDocument(doc);
    iw.close();
  }
  
  public void testRIDFPruning() throws Exception {
    RAMDirectory targetDir = new RAMDirectory();
    CompositeReader topLevelReader = DirectoryReader.open(sourceDir);
    AtomicReader in = SlowCompositeReaderWrapper.wrap(topLevelReader);
    // remove only very popular terms
    RIDFTermPruningPolicy ridf = new RIDFTermPruningPolicy(in, null, null, -0.12);
    PruningAtomicReader tfr = new PruningAtomicReader(in, null, ridf);
    assertTDCount(tfr, new Term("body", "one"), 0);
    assertTD(tfr, new Term("body", "two"), new int[]{0, 1, 2, 4});
    assertTD(tfr, new Term("body", "three"), new int[]{0, 1, 3});
    assertTD(tfr, new Term("test", "one"), new int[]{4});
    assertTD(tfr, new Term("body", "four"), new int[]{0});
    assertTD(tfr, new Term("test", "four"), new int[]{4});
    in.close();
    topLevelReader.close();
  }

  public void testTfPruning() throws Exception {
    RAMDirectory targetDir = new RAMDirectory();
    CompositeReader topLevelReader = DirectoryReader.open(sourceDir);
    AtomicReader in = SlowCompositeReaderWrapper.wrap(topLevelReader);
    TFTermPruningPolicy tfp = new TFTermPruningPolicy(in, null, null, 2);
    PruningAtomicReader tfr = new PruningAtomicReader(in, null, tfp);
    // verify
    assertTD(tfr, new Term("body", "one"), new int[]{1, 2, 3, 4});
    assertTD(tfr, new Term("body", "two"), new int[]{1, 2});
    assertTD(tfr, new Term("body", "three"), new int[]{1, 3});
    assertTD(tfr, new Term("test", "one"), new int[]{4});
    assertTDCount(tfr, new Term("body", "four"), 0);
    assertTDCount(tfr, new Term("test", "four"), 0);
    // verify new reader
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(targetDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    iw.addIndexes(new AtomicReader[]{tfr});
    iw.close();
    DirectoryReader newIndex = DirectoryReader.open(targetDir);
    AtomicReader ir = SlowCompositeReaderWrapper.wrap(newIndex);
    assertTD(ir, new Term("body", "one"), new int[]{1, 2, 3, 4});
    assertTD(ir, new Term("body", "two"), new int[]{1, 2});
    assertTD(ir, new Term("body", "three"), new int[]{1, 3});
    assertTD(ir, new Term("test", "one"), new int[]{4});
    tfr.close();
    ir.close();
    newIndex.close();
    topLevelReader.close();
  }
  
  public void testCarmelTopKPruning() throws Exception {
    CompositeReader topLevelReader = DirectoryReader.open(sourceDir);
    AtomicReader in = SlowCompositeReaderWrapper.wrap(topLevelReader);
    // validate full scores - without pruning, just to make sure we test the right thing
    validateDocScores(fullScores, in, false, false); // validate both docs and scores
    // prune reader
    CarmelTopKTermPruningPolicy tfp = new CarmelTopKTermPruningPolicy(in, null, K, EPSILON, R, null);
    PruningAtomicReader tfr = new PruningAtomicReader(in, null, tfp);
    
    // create the pruned index
    RAMDirectory targetDir = new RAMDirectory();
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(targetDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    iw.addIndexes(new AtomicReader[]{tfr});
    iw.close();
    in.close();

    // validate scores of pruned index
    AtomicReader ir = SlowCompositeReaderWrapper.wrap(topLevelReader);
    validateDocScores(prunedScores, ir, false, true); // validated only docs (scores have changed after pruning)
    ir.close();

    topLevelReader.close();
  }
  
  private void validateDocScores(Map<Term,ScoreDoc[]> baseScores, AtomicReader in, boolean print, boolean onlyDocs) throws IOException {
    validateDocScores(baseScores, in, new Term("body", "one"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("body", "two"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("body", "three"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("test", "one"), print, onlyDocs);
    validateDocScores(baseScores, in, new Term("allthesame", "allthesame"), print, onlyDocs);
  }
  
  /** validate the doc-scores, optionally also print them */
  private void validateDocScores(Map<Term,ScoreDoc[]> baseScores, AtomicReader in, Term term, boolean print, boolean onlyDocs) throws IOException {
    if (print) {
      printDocScores(baseScores, in, term);
    }
    float delta = .0001f;
    IndexSearcher is = new IndexSearcher(in);
    TermQuery q = new TermQuery(term);
    ScoreDoc[] sd = is.search(q, 100).scoreDocs;
    assertNotNull("unknown result for term: "+term, baseScores.get(term));
    assertEquals("wrong number of results!", baseScores.get(term).length, sd.length);
    for (int i = 0; i < sd.length; i++) {
      assertEquals("wrong doc!", baseScores.get(term)[i].doc, sd[i].doc);
      if (!onlyDocs) {
        assertEquals("wrong score!", baseScores.get(term)[i].score, sd[i].score, delta);
      }
    }
  }

  /** Print the doc scores (in a code format */
  private void printDocScores(Map<Term,ScoreDoc[]> baseScores, AtomicReader in, Term term) throws IOException {
    IndexSearcher is = new IndexSearcher(in);
    TermQuery q = new TermQuery(term);
    ScoreDoc[] scoreDocs = is.search(q, 100).scoreDocs;
    System.out.println("t = new Term(\""+term.field+"\",\""+term.text()+"\");");
    System.out.println("sd = new ScoreDoc[] {");
    for (ScoreDoc sd : scoreDocs) {
      System.out.println("    new ScoreDoc("+sd.doc+", "+sd.score+"f),");
    }
    System.out.println("res.put(t,sd);");
  }

  public void testThresholds() throws Exception {
    Map<String, Integer> thresholds = new HashMap<String, Integer>();
    thresholds.put("test", 3);
    CompositeReader topLevelReader = DirectoryReader.open(sourceDir);
    AtomicReader in = SlowCompositeReaderWrapper.wrap(topLevelReader);
    TFTermPruningPolicy tfp = new TFTermPruningPolicy(in, null, thresholds, 2);
    PruningAtomicReader tfr = new PruningAtomicReader(in, null, tfp);
    assertTDCount(tfr, new Term("test", "one"), 0);
    assertTDCount(tfr, new Term("test", "two"), 0);
    assertTD(tfr, new Term("test", "three"), new int[]{4});
    assertTDCount(tfr, new Term("test", "four"), 0);
    in.close();
    topLevelReader.close();
  }
  
  public void testRemoveFields() throws Exception {
    RAMDirectory targetDir = new RAMDirectory();
    Map<String, Integer> removeFields = new HashMap<String, Integer>();
    removeFields.put("test", PruningPolicy.DEL_POSTINGS | PruningPolicy.DEL_STORED);
    CompositeReader topLevelReader = DirectoryReader.open(sourceDir);
    AtomicReader in = SlowCompositeReaderWrapper.wrap(topLevelReader);
    TFTermPruningPolicy tfp = new TFTermPruningPolicy(in, removeFields, null, 2);
    StorePruningPolicy stp = new StorePruningPolicy(in, removeFields);
    PruningAtomicReader tfr = new PruningAtomicReader(in, stp, tfp);
    StoredDocument doc = tfr.document(4);
    // removed stored values?
    assertNull(doc.get("test"));
    // removed postings ?
    Terms terms = tfr.fields().terms("test");
    assertEquals(-1, terms.size());

    // but vectors should be present !
    Terms tv = tfr.getTermVector(4, "test");
    assertNotNull(tv);
    assertEquals(4, tv.size()); // term "four" not deleted yet from TermEnum
    // verify new reader
    WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer(TEST_VERSION_CURRENT);
    IndexWriter iw = new IndexWriter(targetDir, new IndexWriterConfig(TEST_VERSION_CURRENT, analyzer));
    iw.addIndexes(new AtomicReader[]{tfr});
    iw.close();
    AtomicReader ir = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(targetDir));
    tv = ir.getTermVector(4, "test");
    assertNotNull(tv);
    assertEquals(3, tv.size()); // term "four" was deleted from TermEnum
  }

}
