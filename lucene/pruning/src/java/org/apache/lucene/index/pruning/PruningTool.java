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


import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.CompositeReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.PruningAtomicReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

/**
 * A command-line tool to configure and run a {@link org.apache.lucene.index.PruningAtomicReader} on an input
 * index and produce a pruned output index using
 * {@link IndexWriter#addIndexes(IndexReader...)}.
 */
public class PruningTool {

  public static void main(String[] args) throws Exception {
    int res = run(args);
    System.exit(res);
  }
  
  public static int run(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.println("Usage: PruningTool -impl (tf | carmel | carmeltopk | ridf) (-indexReader <path1> [-indexReader <path2> ...]) " +
          "-out <outPath> -t <NN> [-del f1,f2,..] [-conf <file>] [-topkk <NN>] [-topke <NN>] [-topkr <NN>]");
      System.err.println("\t-impl (tf | carmel | carmeltopk | ridf)\tTermPruningPolicy implementation name: TF or CarmelUniform or or CarmelTopK or RIDFTerm");
      System.err.println("\t-indexReader path\tpath to the input index. Can specify multiple input indexes.");
      System.err.println("\t-out path\toutput path where the output index will be stored.");
      System.err.println("\t-t NN\tdefault threshold value (minimum indexReader-document frequency) for all terms");
      System.err.println("\t-del f1,f2,..\tcomma-separated list of field specs to delete (postings, vectors & stored):");
      System.err.println("\t\tfield spec : fieldName ( ':' [pPsv] )");
      System.err.println("\t\twhere: p - postings, P - payloads, s - stored value, v - vectors");
      System.err.println("\t-conf file\tpath to config file with per-term thresholds");
      System.err.println("\t-topkk NN\t'K' for Carmel TopK Pruning: number of guaranteed top scores");
      System.err.println("\t-topke NN\t'Epsilon' for Carmel TopK Pruning: largest meaningless score difference");
      System.err.println("\t-topkr NN\t'R' for Carmel TopK Pruning: planned maximal number of terms indexReader a query on pruned index");
      return -1;
    }

    List<CompositeReader> inputs = new ArrayList<CompositeReader>();
    Directory out = null;
    float thr = -1;
    Map<String, Integer> delFields = new HashMap<String, Integer>();
    
    // parameters for top-K pruning 
    int topkK = CarmelTopKTermPruningPolicy.DEFAULT_TOP_K;
    float topkEpsilon = CarmelTopKTermPruningPolicy.DEFAULT_EPSILON;
    int topkR = CarmelTopKTermPruningPolicy.DEFAULT_R;
    
    String impl = null;
    for (int i = 0; i < args.length; i++) {
      if (args[i].equals("-indexReader")) {
        Directory d = FSDirectory.open(new File(args[++i]));
        if (!DirectoryReader.indexExists(d)) {
          System.err.println("WARN: no index indexReader " + args[i] + ", skipping ...");
        }
        inputs.add(DirectoryReader.open(d));
      } else if (args[i].equals("-out")) {
        File outFile = new File(args[++i]);
        if (outFile.exists()) {
          throw new Exception("Output " + outFile + " already exists.");
        }
        //noinspection ResultOfMethodCallIgnored
        outFile.mkdirs();
        out = FSDirectory.open(outFile);
      } else if (args[i].equals("-impl")) {
        impl = args[++i];
      } else if (args[i].equals("-t")) {
        thr = Float.parseFloat(args[++i]);
      } else if (args[i].equals("-topkk")) {
        topkK = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-topke")) {
        topkEpsilon = Float.parseFloat(args[++i]);
      } else if (args[i].equals("-topkr")) {
        topkR = Integer.parseInt(args[++i]);
      } else if (args[i].equals("-del")) {
        String[] fields = args[++i].split(",");
        for (String f : fields) {
          // parse field spec
          String[] spec = f.split(":");
          int opts = PruningPolicy.DEL_ALL;
          if (spec.length > 0) {
            opts = 0;
            if (spec[1].indexOf('p') != -1) {
              opts |= PruningPolicy.DEL_POSTINGS;
            }
            if (spec[1].indexOf('P') != -1) {
              opts |= PruningPolicy.DEL_PAYLOADS;
            }
            if (spec[1].indexOf('s') != -1) {
              opts |= PruningPolicy.DEL_STORED;
            }
            if (spec[1].indexOf('v') != -1) {
              opts |= PruningPolicy.DEL_VECTOR;
            }
          }
          delFields.put(spec[0], opts);
        }
      } else if (args[i].equals("-conf")) {
        ++i;
        System.err.println("WARN: -conf option not implemented yet.");
      } else {
        throw new Exception("Invalid argument: '" + args[i] + "'");
      }
    }
    if (impl == null) {
      throw new Exception("Must select algorithm implementation");
    }
    if (inputs.size() == 0) {
      throw new Exception("At least one input index is required.");
    }
    if (out == null) {
      throw new Exception("Output path is not set.");
    }
    if (thr == -1) {
      throw new Exception("Threshold value is not set.");
    }

    /*
    IndexReader indexReader;
    if (inputs.size() == 1) {
      indexReader = inputs.get(0);
    } else {
      indexReader = new MultiReader(inputs.toArray(new IndexReader[inputs.size()]), true);
    }
    */

    for (CompositeReader reader : inputs) {
      if (reader.hasDeletions()) {
        System.err.println("WARN: input index(es) with deletions - document ID-s will NOT be preserved!");
      }

      IndexReader pruning = null;
      StorePruningPolicy stp = null;

      // TODO [Greg Bowyer] I dont see why we cant avoid this, but as a starter for ten
      // While we transliterate the source code its a start
      AtomicReader indexReader = SlowCompositeReaderWrapper.wrap(reader);

      if (delFields.size() > 0) {
        stp = new StorePruningPolicy(reader, delFields);
      }

      TermPruningPolicy tpp = null;

      // TODO [Greg Bowyer] Can we make this a little nicer somehow ?
      if (impl.equals("tf")) {
        tpp = new TFTermPruningPolicy(indexReader, delFields, null, (int)thr);
      } else if (impl.equals("carmel")) {
        tpp = new CarmelUniformTermPruningPolicy(indexReader, delFields, null, thr, null);
      } else if (impl.equals("carmeltopk")) {
        tpp = new CarmelTopKTermPruningPolicy(indexReader, delFields, topkK, topkEpsilon, topkR, null);
      } else if (impl.equals("ridf")) {
        tpp = new RIDFTermPruningPolicy(indexReader, delFields, null, thr);
      } else {
        throw new Exception("Unknown algorithm: '" + impl + "'");
      }

      pruning = new PruningAtomicReader(indexReader, stp, tpp);
      IndexWriterConfig cfg = new IndexWriterConfig(Version.LUCENE_50,
              new WhitespaceAnalyzer(Version.LUCENE_50));
      IndexWriter iw = new IndexWriter(out, cfg);
      try {
        iw.addIndexes(pruning);
      } finally {
        iw.close();
      }
    }

    System.err.println("DONE.");
    return 0;
  }
}
