/*
 * Copyright 2012 The jASN1 Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.beanit.jasn1.ber.internal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

public class Util {

  public static void readFully(InputStream is, byte[] buffer) throws IOException {
    readFully(is, buffer, 0, buffer.length);
  }

  public static void readFully(InputStream is, byte[] buffer, int off, int len) throws IOException {
    do {
      int bytesRead = is.read(buffer, off, len);
      if (bytesRead == -1) {
        throw new EOFException("Unexpected end of input stream.");
      }

      len -= bytesRead;
      off += bytesRead;
    } while (len > 0);
  }
}
