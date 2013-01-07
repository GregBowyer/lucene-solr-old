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
package org.apache.lucene.store;

import java.io.File;

import org.apache.lucene.store.Directory.IndexInputSlicer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util._TestUtil;

/**
 * Tests the native posix mmap directory
 */
public class TestNativePosixMMapDirectory extends LuceneTestCase {

  private File workDir;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    assumeTrue("test requires a posix compliant OS", Constants.POSIX);
    assumeTrue("test requires the native library to be in the java lib path", NativePosixUtil.isLibLoaded());
    workDir = _TestUtil.getTempDir("TestNativePosixMMapDirectory");
    workDir.mkdirs();
  }

  public void testCloneSafety() throws Exception {
    NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testCloneSafety"));
    IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
    io.writeVInt(5);
    io.close();
    IndexInput one = mmapDir.openInput("bytes", IOContext.DEFAULT);
    IndexInput two = one.clone();
    IndexInput three = two.clone(); // clone of clone
    one.close();
    try {
      one.readVInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    try {
      two.readVInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    try {
      three.readVInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    two.close();
    three.close();
    // test double close of master:
    one.close();
    mmapDir.close();
  }

  public void testCloneClose() throws Exception {
    NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testCloneClose"));
    IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
    io.writeVInt(5);
    io.close();
    IndexInput one = mmapDir.openInput("bytes", IOContext.DEFAULT);
    IndexInput two = one.clone();
    IndexInput three = two.clone(); // clone of clone
    two.close();
    assertEquals(5, one.readVInt());
    try {
      two.readVInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    assertEquals(5, three.readVInt());
    one.close();
    three.close();
    mmapDir.close();
  }

  public void testCloneSliceSafety() throws Exception {
    NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testCloneSliceSafety"));
    IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
    io.writeInt(1);
    io.writeInt(2);
    io.close();
    IndexInputSlicer slicer = mmapDir.createSlicer("bytes", newIOContext(random()));
    IndexInput one = slicer.openSlice("first int", 0, 4);
    IndexInput two = slicer.openSlice("second int", 4, 4);
    IndexInput three = one.clone(); // clone of clone
    IndexInput four = two.clone(); // clone of clone
    slicer.close();
    try {
      one.readInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    try {
      two.readInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    try {
      three.readInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    try {
      four.readInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    one.close();
    two.close();
    three.close();
    four.close();
    // test double-close of slicer:
    slicer.close();
    mmapDir.close();
  }

  public void testCloneSliceClose() throws Exception {
    NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testCloneSliceClose"));
    IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
    io.writeInt(1);
    io.writeInt(2);
    io.close();
    IndexInputSlicer slicer = mmapDir.createSlicer("bytes", newIOContext(random()));
    IndexInput one = slicer.openSlice("first int", 0, 4);
    IndexInput two = slicer.openSlice("second int", 4, 4);
    one.close();
    try {
      one.readInt();
      fail("Must throw AlreadyClosedException");
    } catch (AlreadyClosedException ignore) {
      // pass
    }
    assertEquals(2, two.readInt());
    // reopen a new slice "one":
    one = slicer.openSlice("first int", 0, 4);
    assertEquals(1, one.readInt());
    one.close();
    two.close();
    slicer.close();
    mmapDir.close();
  }

  public void testSeekZero() throws Exception {
    for (int i = 0; i < 31; i++) {
      NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testSeekZero"), null);
      IndexOutput io = mmapDir.createOutput("zeroBytes", newIOContext(random()));
      io.close();
      IndexInput ii = mmapDir.openInput("zeroBytes", newIOContext(random()));
      ii.seek(0L);
      ii.close();
      mmapDir.close();
    }
  }

  public void testSeekSliceZero() throws Exception {
    for (int i = 0; i < 31; i++) {
      NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testSeekSliceZero"), null);
      IndexOutput io = mmapDir.createOutput("zeroBytes", newIOContext(random()));
      io.close();
      IndexInputSlicer slicer = mmapDir.createSlicer("zeroBytes", newIOContext(random()));
      IndexInput ii = slicer.openSlice("zero-length slice", 0, 0);
      ii.seek(0L);
      ii.close();
      slicer.close();
      mmapDir.close();
    }
  }

  public void testSeekEnd() throws Exception {
    for (int i = 0; i < 17; i++) {
      NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testSeekEnd"), null);
      IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
      byte bytes[] = new byte[1<<i];
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = mmapDir.openInput("bytes", newIOContext(random()));
      byte actual[] = new byte[1<<i];
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      ii.seek(1<<i);
      ii.close();
      mmapDir.close();
    }
  }

  public void testSeekSliceEnd() throws Exception {
    for (int i = 0; i < 17; i++) {
      NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testSeekSliceEnd"), null);
      IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
      byte bytes[] = new byte[1<<i];
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInputSlicer slicer = mmapDir.createSlicer("bytes", newIOContext(random()));
      IndexInput ii = slicer.openSlice("full slice", 0, bytes.length);
      byte actual[] = new byte[1<<i];
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      ii.seek(1<<i);
      ii.close();
      slicer.close();
      mmapDir.close();
    }
  }

  public void testSeeking() throws Exception {
    for (int i = 0; i < 10; i++) {
      NativePosixMMapDirectory mmapDir = new NativePosixMMapDirectory(_TestUtil.getTempDir("testSeeking"), null);
      IndexOutput io = mmapDir.createOutput("bytes", newIOContext(random()));
      byte bytes[] = new byte[1<<(i+1)]; // make sure we switch buffers
      random().nextBytes(bytes);
      io.writeBytes(bytes, bytes.length);
      io.close();
      IndexInput ii = mmapDir.openInput("bytes", newIOContext(random()));
      byte actual[] = new byte[1<<(i+1)]; // first read all bytes
      ii.readBytes(actual, 0, actual.length);
      assertEquals(new BytesRef(bytes), new BytesRef(actual));
      for (int sliceStart = 0; sliceStart < bytes.length; sliceStart++) {
        for (int sliceLength = 0; sliceLength < bytes.length - sliceStart; sliceLength++) {
          byte slice[] = new byte[sliceLength];
          ii.seek(sliceStart);
          ii.readBytes(slice, 0, slice.length);
          assertEquals(new BytesRef(bytes, sliceStart, sliceLength), new BytesRef(slice));
        }
      }
      ii.close();
      mmapDir.close();
    }
  }

}
