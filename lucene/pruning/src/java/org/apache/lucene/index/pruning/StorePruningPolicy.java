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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.logging.Logger;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFieldVisitor;

/**
 * Pruning policy for removing stored fields from documents.
 */
public class StorePruningPolicy extends PruningPolicy {
  
  private static final Logger LOG = Logger.getLogger(StorePruningPolicy.class.getName());
  
  /** Pruning in effect for each field */ 
  protected Map<String,Integer> fieldFlags;
  
  /** Fields to be completely deleted */
  protected Set<String> deleteAll;
  
  protected DelFieldSelector fs;
  protected IndexReader in;
  protected int delFields; // total number of fields deleted
  
  /**
   * Constructs a policy.
   * @param in input reader.
   * @param fieldFlags a map where keys are field names, and flags are
   * bitwise-OR values of flags defined in {@link PruningPolicy}.
   */
  public StorePruningPolicy(IndexReader in, Map<String,Integer> fieldFlags) {
    if (fieldFlags != null) {
      this.fieldFlags = fieldFlags;
      deleteAll = new HashSet<String>();
      for (Entry<String,Integer> e : fieldFlags.entrySet()) {
        if (e.getValue() == PruningPolicy.DEL_ALL) {
          deleteAll.add(e.getKey());
        }
      }
    } else {
      this.fieldFlags = Collections.emptyMap();
      deleteAll = Collections.emptySet();
    }
    fs = new DelFieldSelector(fieldFlags);
    this.in = in;
  }
  
  /**
   * Compute field infos that should be retained
   * @param allInfos original field infos 
   * @return those of the original field infos which should not be removed.
   */
  public FieldInfos getFieldInfos(FieldInfos allInfos) {
    // for simplicity remove only fields with DEL_ALL
    List<FieldInfo> res = new ArrayList<FieldInfo>(allInfos.size());
    for (FieldInfo fi: allInfos) {
      if (!deleteAll.contains(fi.name)) {
        res.add(fi);
      }
    }

    FieldInfo[] infos = new FieldInfo[res.size()];
    res.toArray(infos);
    return new FieldInfos(infos);
  }
  
  /**
   * Prune stored fields of a document. Note that you can also arbitrarily
   * change values of the retrieved fields, so long as the field names belong
   * to a list of fields returned from {@link #getFieldInfos(FieldInfos)}.
   * @param doc document number
   * @param visitor original field selector that limits what fields will be
   * retrieved.
   * @throws IOException
   */
  public void pruneDocument(int doc, StoredFieldVisitor visitor) throws IOException {
    if (fieldFlags.isEmpty()) {
      in.document(doc, visitor);
    } else {
      fs.setParent(visitor);
      in.document(doc, fs);
    }    
  }
  
  class DelFieldSelector extends StoredFieldVisitor {
    private static final long serialVersionUID = -4913592063491685103L;
    private StoredFieldVisitor parent;
    private Map<String, Integer> remove;
    
    public DelFieldSelector(Map<String,Integer> remove) {
      this.remove = remove;
    }
    
    public void setParent(StoredFieldVisitor parent) {
      this.parent = parent;
    }
    
    @Override
    public Status needsField(FieldInfo fieldInfo) throws IOException {
      String fieldName = fieldInfo.name;
      if (!remove.isEmpty() && remove.containsKey(fieldName) && ((remove.get(fieldName) & DEL_STORED) > 0)) {
        delFields++;
        if (delFields % 10000 == 0) {
          LOG.info(" - stored fields: removed " + delFields + " fields.");
        }
        return Status.NO;
      } else if (parent != null) {
        return parent.needsField(fieldInfo);
      } else {
        return Status.YES;
      }

    }

  };

}
