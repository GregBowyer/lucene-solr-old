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

package org.apache.solr.handler.component;

import org.apache.solr.common.params.CollectionParams;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

public class CollectorSpec implements Comparable{
    private static final CollectorSpec[] EMPTY_DELGATING = new CollectorSpec[0];
    private CollectorSpec[] delegating = EMPTY_DELGATING;
    private String name;
    private String signiture;
    private int id;
    private Map<String, String> params = new HashMap<String, String>();

    public CollectorSpec() {
       super();
    }

    public CollectorSpec(String name) {
        this.name = this.signiture = name;
    }

    public int compareTo(Object o) {
        CollectorSpec cs = (CollectorSpec)o;
        if(id == cs.id) {
            return 0;
        } else if (id < cs.id) {
            return -1;
        }
        else
        {
            return 1;
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return this.id;
    }

    public void setSigniture(String signiture) {
        this.signiture = signiture;
    }

    public String getSigniture() {
        return this.signiture;
    }

    public CollectorSpec[] getDelegating() {
        return this.delegating;
    }

    public void setDelegating(CollectorSpec[] delegating) {
        this.delegating = delegating;
    }

    public int hashCode() {
        int h = signiture.hashCode();
        for(int i=0; i<delegating.length; i++) {
            h += delegating[i].signiture.hashCode();
        }

        return h;
    }

    public boolean equals(Object o) {
        CollectorSpec cs = (CollectorSpec) o;
        if(signiture.equals(cs.signiture)) {
            if(delegating.length == cs.delegating.length) {
                for(int i=0; i<delegating.length; i++) {
                    if(!delegating[i].signiture.equals(cs.delegating[i].signiture)) {
                        return false;
                    }
                }
            } else {
                return false;
            }
        } else {
            return false;
        }

        return true;
    }

    public void setParams(Map<String, String> params) {
        this.params = params;
    }

    public Map<String, String> getParams() {
        return this.params;
    }
}
