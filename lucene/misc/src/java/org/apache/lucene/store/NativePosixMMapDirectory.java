package org.apache.lucene.store;

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

import org.apache.lucene.util.Constants;
import org.apache.lucene.util.WeakIdentityMap;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.WeakHashMap;

import static java.lang.String.format;
import static org.apache.lucene.util.Guarantee.ensureInstanceOf;
import static org.apache.lucene.util.Guarantee.ensureIsFalse;
import static org.apache.lucene.util.Guarantee.ensureIsTrue;

/**
 * Implementation of file base {@link Directory} that directly uses the mmap(2)
 * system call for reading, and {@link FSDirectory.FSIndexOutput} for writing.
 *
 * <p><b>NOTE</b>: memory mapping uses up a portion of the virtual memory
 * address space in your process equal to the length of the file being mapped.
 * Before using this class, be sure your have plenty of virtual address space,
 * this implementation assumes a 64bit virtual machine and will fail if the VM is
 * found to be 32bit. This implementation is also only assumed to work on posix
 * compliant operating systems, and as such presently does not work on windows.
 *
 * <p>This will consume additional transient disk usage, and may lead to page
 * faults in the OS.
 *
 */
public class NativePosixMMapDirectory extends FSDirectory {

  /**
   * Create a new MMapDirectory for the named location and {@link NativeFSLockFactory}.
   * @param path the path of the directory
   * @throws IOException if there is a low-level I/O error
   */
  public NativePosixMMapDirectory(File path) throws IOException {
    this(path, null);
  }

  /**
   * Create a new MMapDirectory for the named location.
   * @param path the path of the directory
   * @param lockFactory the lock factory to use, or null for the default
   * ({@link NativeFSLockFactory});
   * @throws IOException if there is a low-level I/O error
   */
  public NativePosixMMapDirectory(File path, LockFactory lockFactory) throws IOException {
    super(path, lockFactory);

    ensureIsTrue(Constants.JRE_IS_64BIT, IOException.class, "The native posix mmap directory assumes a 64bit VM");
    ensureIsFalse(Constants.WINDOWS, IOException.class, "This directory impl is unix specific");
  }

  /** Creates an IndexInput for the file with the given name. */
  @Override
  public IndexInput openInput(String name, IOContext context) throws IOException {
    ensureOpen();
    String path = new File(getDirectory(), name).getAbsolutePath();
    RandomAccessFile raf = null;
    try {
      raf = new RandomAccessFile(path, "r");
      return new NativePosixMMapIndexInput(path, raf, context, false);
    } finally {
      if (raf != null) {
        raf.close();
      }
    }
  }

  private static final class NativePosixMMapIndexInput extends IndexInput {

    private final static Unsafe unsafe;
    static {
      try {
        Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
        theUnsafe.setAccessible(true);
        unsafe = (Unsafe) theUnsafe.get(null);
      } catch (Exception e) {
        throw new ExceptionInInitializerError("Could not acquire the Unsafe");
      }
    }

    // Cached array base offset
    private static final long arrayBaseOffset = (long)unsafe.arrayBaseOffset(byte[].class);

    // Idea taken from hotspot - size of bytes that where the cost of doing Unsafe.getBytes()
    // (which is basically memcpy) is more expensive than the VM intrinsic of doing an
    // unsafe pointer read - For us its usage is probably more that we can warn about
    // ineffective I/O operations
    private static final int SMALL_COPY_THRESHOLD = 6;

    // Limit that is used to ensure that the JVM is able to get onto a safepoint without
    // being locked up by calls to memcpy() this idea is taken from hotspot, although this
    // value might want to be adjusted to change the behaviour wrt to I/O effectiveness in
    // lucene
    private static final long SAFEPOINT_COPY_THRESHOLD = 1024L * 1024L;

    // Used to control if we want to allow the VM to memcpy() in 1MB amounts, this is for
    // experimental testing and will probably be removed from the final version
    private final boolean yieldForVMSafePointing;

    // Clones will make the VM go BOOM (SEGV) if we attempt to read bytes after the fact
    private final boolean isClone;
    private final WeakIdentityMap<NativePosixMMapIndexInput, Boolean> clones;

    private final long mappingAddress;
    private final long length;

    private long offset;
    private volatile boolean isOpen = false;

    public NativePosixMMapIndexInput(String path, RandomAccessFile file, IOContext context, boolean yieldForSafePoints) throws IOException {
      super(format("NativePosixMMapIndexInput(path=%s)", path));
      this.yieldForVMSafePointing = yieldForSafePoints;
      this.length = file.length();

      this.isClone = false;
      this.clones = WeakIdentityMap.newConcurrentHashMap();

      if (this.length() > 0) {
        // mmap all the things
        this.mappingAddress = NativePosixUtil.mmap(file.getFD(), this.length);
      } else {
        this.mappingAddress = -1;
      }
      this.isOpen = true;
    }

    public NativePosixMMapIndexInput(NativePosixMMapIndexInput clonee) {
      super(clonee.toString());
      ensureIsTrue(clonee.isOpen, AlreadyClosedException.class);
      this.yieldForVMSafePointing = clonee.yieldForVMSafePointing;
      this.length = clonee.length;

      this.clones = clonee.clones;
      this.mappingAddress = clonee.mappingAddress;
      this.isOpen = clonee.isOpen;
      this.isClone = true;
      this.offset = clonee.offset;
    }

    @Override
    public void close() throws IOException {
      // munmap all the things
      if (isOpen && !isClone) {

        if (this.mappingAddress != -1) {
          NativePosixUtil.munmap(this.mappingAddress, this.length);
        }

        // I think this is a potential race condition, does lucene ensure that clones are
        // not used concurrently to a closed index input ?
        for (Iterator<NativePosixMMapIndexInput> it = this.clones.keyIterator(); it.hasNext();) {
          final NativePosixMMapIndexInput clone = it.next();
          ensureIsTrue(clone.isClone, "Trying to indicate closure on a none clone");
          clone.isOpen = false;
        }
        this.clones.clear();
      }
      this.isOpen = false;
    }

    @Override
    public long getFilePointer() {
      ensureIsTrue(isOpen, AlreadyClosedException.class);
      return this.offset;
    }

    @Override
    public void seek(long pos) throws IOException {
      ensureIsTrue(isOpen, AlreadyClosedException.class);
      ensureIsFalse(pos < 0, IOException.class, "Attempt to make a negative seek");
      ensureIsTrue(pos <= length, IOException.class, "Attempting to seek beyond the end of the file");
      this.offset = pos;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public byte readByte() throws IOException {
      ensureIsTrue(isOpen, AlreadyClosedException.class);
      ensureIsFalse(this.offset >= length, IOException.class, "read past EOF");
      return unsafe.getByte(trueAddress(this.offset++));
    }

    @Override
    public void readBytes(final byte[] b, final int destPosition, final int len) throws IOException {
      ensureIsTrue(isOpen, AlreadyClosedException.class);
      ensureIsFalse(b.length < len, "Requested to copy more bytes than array allows");
      ensureIsFalse(len > this.length - this.offset, IOException.class, "Attempt to read past end of mmap area");

      // checkbounds ?
      long srcAddr = trueAddress(this.offset);
      long copyLen = len;

      if (len < SMALL_COPY_THRESHOLD) {
        // TODO - Should be logged
        //log.warn("Small I/O copy of {} bytes, this can be inefficient", len);

        for (int i=0; i < b.length; i++) {
          b[i] = unsafe.getByte(srcAddr);
          copyLen--;
          srcAddr++;
        }
      } else {
        long destOffset = arrayBaseOffset + destPosition;
        while (copyLen > 0) {
          long size = (this.yieldForVMSafePointing && length > SAFEPOINT_COPY_THRESHOLD) ?
              SAFEPOINT_COPY_THRESHOLD : len;
          unsafe.copyMemory(null, srcAddr, b, destOffset, size);
          copyLen -= size;
          srcAddr += size;
          destOffset += size;
        }
      }

      this.offset += len;
    }

    private long trueAddress(long pos) {
      return this.mappingAddress + pos;
    }

    @Override
    public IndexInput clone() {
      NativePosixMMapIndexInput toReturn = new NativePosixMMapIndexInput(this);
      clones.put(toReturn, true);
      return toReturn;
    }
  }
}
