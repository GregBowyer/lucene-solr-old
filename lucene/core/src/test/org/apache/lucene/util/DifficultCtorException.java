package org.apache.lucene.util;

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

/**
 * Exception at has and awkward ctor designed to make it
 * more difficult to obtain at the traditional
 * exception ctor
 */
class DifficultCtorException extends Exception {

    // Provide only the extended ctor, and one
    // with an arbitrary argument
    public DifficultCtorException(String message, Throwable cause) {
        super(message, cause);
    }

    public DifficultCtorException(String message, Integer exceptionVersion) {
        super(message);
    }
}
